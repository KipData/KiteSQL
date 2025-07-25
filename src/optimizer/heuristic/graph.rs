use crate::optimizer::core::memo::Memo;
use crate::planner::operator::Operator;
use crate::planner::{Childrens, LogicalPlan};
use fixedbitset::FixedBitSet;
use itertools::Itertools;
use petgraph::stable_graph::{NodeIndex, StableDiGraph};
use petgraph::visit::{Bfs, EdgeRef};
use std::mem;

/// HepNodeId is used in optimizer to identify a node.
pub type HepNodeId = NodeIndex<usize>;

#[derive(Debug, Clone)]
pub struct HepGraph {
    graph: StableDiGraph<Operator, usize, usize>,
    root_index: HepNodeId,
    pub version: usize,
}

impl HepGraph {
    pub fn new(root: LogicalPlan) -> Self {
        fn graph_filling(
            graph: &mut StableDiGraph<Operator, usize, usize>,
            LogicalPlan {
                operator,
                childrens,
                ..
            }: LogicalPlan,
        ) -> HepNodeId {
            let index = graph.add_node(operator);

            match *childrens {
                Childrens::None => (),
                Childrens::Only(child) => {
                    let child_index = graph_filling(graph, child);
                    let _ = graph.add_edge(index, child_index, 0);
                }
                Childrens::Twins { left, right } => {
                    let child_index = graph_filling(graph, left);
                    let _ = graph.add_edge(index, child_index, 0);
                    let child_index = graph_filling(graph, right);
                    let _ = graph.add_edge(index, child_index, 1);
                }
            }
            index
        }

        let mut graph = StableDiGraph::<Operator, usize, usize>::default();

        let root_index = graph_filling(&mut graph, root);

        HepGraph {
            graph,
            root_index,
            version: 0,
        }
    }

    pub fn node_count(&self) -> usize {
        self.graph.node_count()
    }

    pub fn parent_id(&self, node_id: HepNodeId) -> Option<HepNodeId> {
        self.graph
            .neighbors_directed(node_id, petgraph::Direction::Incoming)
            .next()
    }

    #[allow(dead_code)]
    pub fn add_root(&mut self, new_node: Operator) {
        let old_root_id = mem::replace(&mut self.root_index, self.graph.add_node(new_node));

        self.graph.add_edge(self.root_index, old_root_id, 0);
        self.version += 1;
    }

    pub fn add_node(
        &mut self,
        source_id: HepNodeId,
        children_option: Option<HepNodeId>,
        new_node: Operator,
    ) -> HepNodeId {
        let new_index = self.graph.add_node(new_node);
        let mut order = self.graph.edges(source_id).count();

        if let Some((children_id, old_edge_id)) = children_option.and_then(|children_id| {
            self.graph
                .find_edge(source_id, children_id)
                .map(|old_edge_id| (children_id, old_edge_id))
        }) {
            order = self.graph.remove_edge(old_edge_id).unwrap_or(0);

            self.graph.add_edge(new_index, children_id, 0);
        }

        self.graph.add_edge(source_id, new_index, order);
        self.version += 1;
        new_index
    }

    pub fn replace_node(&mut self, source_id: HepNodeId, new_node: Operator) {
        self.graph[source_id] = new_node;
        self.version += 1;
    }

    pub fn swap_node(&mut self, a: HepNodeId, b: HepNodeId) {
        let tmp = self.graph[a].clone();

        self.graph[a] = mem::replace(&mut self.graph[b], tmp);
        self.version += 1;
    }

    pub fn remove_node(&mut self, source_id: HepNodeId, with_childrens: bool) -> Option<Operator> {
        if !with_childrens {
            let children_ids = self
                .graph
                .edges(source_id)
                .sorted_by_key(|edge_ref| edge_ref.weight())
                .map(|edge_ref| edge_ref.target())
                .collect_vec();

            if let Some(parent_id) = self.parent_id(source_id) {
                if let Some(edge) = self.graph.find_edge(parent_id, source_id) {
                    let weight = *self.graph.edge_weight(edge).unwrap_or(&0);

                    for (order, children_id) in children_ids.into_iter().enumerate() {
                        let _ = self.graph.add_edge(parent_id, children_id, weight + order);
                    }
                }
            } else {
                debug_assert!(children_ids.len() < 2);
                self.root_index = children_ids[0];
            }
        }

        self.version += 1;
        self.graph.remove_node(source_id)
    }

    #[allow(dead_code)]
    pub fn node(&self, node_id: HepNodeId) -> Option<&Operator> {
        self.graph.node_weight(node_id)
    }

    pub fn operator(&self, node_id: HepNodeId) -> &Operator {
        &self.graph[node_id]
    }

    pub fn operator_mut(&mut self, node_id: HepNodeId) -> &mut Operator {
        &mut self.graph[node_id]
    }

    /// If input node is join, we use the edge weight to control the join children order.
    pub fn children_at(&self, id: HepNodeId) -> Box<dyn Iterator<Item = HepNodeId> + '_> {
        Box::new(
            self.graph
                .edges(id)
                .sorted_by_key(|edge| edge.weight())
                .map(|edge| edge.target()),
        )
    }

    pub fn eldest_child_at(&self, id: HepNodeId) -> Option<HepNodeId> {
        self.graph
            .edges(id)
            .min_by_key(|edge| edge.weight())
            .map(|edge| edge.target())
    }

    pub fn youngest_child_at(&self, id: HepNodeId) -> Option<HepNodeId> {
        self.graph
            .edges(id)
            .max_by_key(|edge| edge.weight())
            .map(|edge| edge.target())
    }

    pub fn into_plan(mut self, memo: Option<&Memo>) -> Option<LogicalPlan> {
        self.build_childrens(self.root_index, memo)
    }

    /// Use bfs to traverse the graph and return node ids
    pub fn nodes_iter(&self, start_option: Option<HepNodeId>) -> HepGraphIter {
        let inner = Bfs::new(&self.graph, start_option.unwrap_or(self.root_index));
        HepGraphIter {
            inner,
            graph: &self.graph,
        }
    }

    fn build_childrens(&mut self, start: HepNodeId, memo: Option<&Memo>) -> Option<LogicalPlan> {
        let physical_option = memo.and_then(|memo| memo.cheapest_physical_option(&start));

        let mut iter = self.children_at(start);

        let child_0 = iter.next();
        let child_1 = iter.next();
        drop(iter);

        let child_0 = child_0.and_then(|id| self.build_childrens(id, memo));
        let child_1 = child_1.and_then(|id| self.build_childrens(id, memo));

        let childrens = match (child_0, child_1) {
            (Some(child_0), Some(child_1)) => Childrens::Twins {
                left: child_0,
                right: child_1,
            },
            (Some(child), None) | (None, Some(child)) => Childrens::Only(child),
            (None, None) => Childrens::None,
        };

        self.graph.remove_node(start).map(|operator| LogicalPlan {
            operator,
            childrens: Box::new(childrens),
            physical_option,
            _output_schema_ref: None,
        })
    }
}

pub struct HepGraphIter<'a> {
    inner: Bfs<HepNodeId, FixedBitSet>,
    graph: &'a StableDiGraph<Operator, usize, usize>,
}

impl Iterator for HepGraphIter<'_> {
    type Item = HepNodeId;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next(self.graph)
    }
}

#[cfg(test)]
mod tests {
    use crate::binder::test::build_t1_table;
    use crate::errors::DatabaseError;
    use crate::optimizer::heuristic::graph::{HepGraph, HepNodeId};
    use crate::planner::operator::Operator;
    use crate::planner::{Childrens, LogicalPlan};
    use petgraph::stable_graph::{EdgeIndex, NodeIndex};

    #[test]
    fn test_graph_for_plan() -> Result<(), DatabaseError> {
        let table_state = build_t1_table()?;
        let plan = table_state.plan("select * from t1 left join t2 on c1 = c3")?;
        let graph = HepGraph::new(plan);

        assert!(graph
            .graph
            .contains_edge(NodeIndex::new(1), NodeIndex::new(2)));
        assert!(graph
            .graph
            .contains_edge(NodeIndex::new(1), NodeIndex::new(3)));
        assert!(graph
            .graph
            .contains_edge(NodeIndex::new(0), NodeIndex::new(1)));

        assert_eq!(graph.graph.edge_weight(EdgeIndex::new(0)), Some(&0));
        assert_eq!(graph.graph.edge_weight(EdgeIndex::new(1)), Some(&1));
        assert_eq!(graph.graph.edge_weight(EdgeIndex::new(2)), Some(&0));

        Ok(())
    }

    #[test]
    fn test_graph_add_node() -> Result<(), DatabaseError> {
        let table_state = build_t1_table()?;
        let plan = table_state.plan("select * from t1 left join t2 on c1 = c3")?;
        let mut graph = HepGraph::new(plan);

        graph.add_node(HepNodeId::new(1), None, Operator::Dummy);

        graph.add_node(HepNodeId::new(1), Some(HepNodeId::new(4)), Operator::Dummy);

        graph.add_node(HepNodeId::new(5), None, Operator::Dummy);

        assert!(graph
            .graph
            .contains_edge(NodeIndex::new(5), NodeIndex::new(4)));
        assert!(graph
            .graph
            .contains_edge(NodeIndex::new(1), NodeIndex::new(5)));
        assert!(graph
            .graph
            .contains_edge(NodeIndex::new(5), NodeIndex::new(6)));

        assert_eq!(graph.graph.edge_weight(EdgeIndex::new(3)), Some(&0));
        assert_eq!(graph.graph.edge_weight(EdgeIndex::new(4)), Some(&2));
        assert_eq!(graph.graph.edge_weight(EdgeIndex::new(5)), Some(&1));

        Ok(())
    }

    #[test]
    fn test_graph_replace_node() -> Result<(), DatabaseError> {
        let table_state = build_t1_table()?;
        let plan = table_state.plan("select * from t1 left join t2 on c1 = c3")?;
        let mut graph = HepGraph::new(plan);

        graph.replace_node(HepNodeId::new(1), Operator::Dummy);

        assert!(matches!(graph.operator(HepNodeId::new(1)), Operator::Dummy));

        Ok(())
    }

    #[test]
    fn test_graph_remove_middle_node_by_single() -> Result<(), DatabaseError> {
        let table_state = build_t1_table()?;
        let plan = table_state.plan("select * from t1 left join t2 on c1 = c3")?;
        let mut graph = HepGraph::new(plan);

        graph.remove_node(HepNodeId::new(1), false);

        assert_eq!(graph.graph.edge_count(), 2);

        assert!(graph
            .graph
            .contains_edge(NodeIndex::new(0), NodeIndex::new(2)));
        assert!(graph
            .graph
            .contains_edge(NodeIndex::new(0), NodeIndex::new(3)));

        Ok(())
    }

    #[test]
    fn test_graph_remove_middle_node_with_childrens() -> Result<(), DatabaseError> {
        let table_state = build_t1_table()?;
        let plan = table_state.plan("select * from t1 left join t2 on c1 = c3")?;
        let mut graph = HepGraph::new(plan);

        graph.remove_node(HepNodeId::new(1), true);

        assert_eq!(graph.graph.edge_count(), 0);

        Ok(())
    }

    #[test]
    fn test_graph_swap_node() -> Result<(), DatabaseError> {
        let table_state = build_t1_table()?;
        let plan = table_state.plan("select * from t1 left join t2 on c1 = c3")?;
        let mut graph = HepGraph::new(plan);

        let before_op_0 = graph.operator(HepNodeId::new(0)).clone();
        let before_op_1 = graph.operator(HepNodeId::new(1)).clone();

        graph.swap_node(HepNodeId::new(0), HepNodeId::new(1));

        let op_0 = graph.operator(HepNodeId::new(0));
        let op_1 = graph.operator(HepNodeId::new(1));

        assert_eq!(op_0, &before_op_1);
        assert_eq!(op_1, &before_op_0);

        Ok(())
    }

    #[test]
    fn test_graph_add_root() -> Result<(), DatabaseError> {
        let table_state = build_t1_table()?;
        let plan = table_state.plan("select * from t1 left join t2 on c1 = c3")?;
        let mut graph = HepGraph::new(plan);

        graph.add_root(Operator::Dummy);

        assert_eq!(graph.graph.edge_count(), 4);
        assert!(graph
            .graph
            .contains_edge(NodeIndex::new(4), NodeIndex::new(0)));
        assert_eq!(graph.graph.edge_weight(EdgeIndex::new(3)), Some(&0));

        Ok(())
    }

    #[test]
    fn test_graph_to_plan() -> Result<(), DatabaseError> {
        fn clear_output_schema_buf(plan: &mut LogicalPlan) {
            plan._output_schema_ref = None;

            match plan.childrens.as_mut() {
                Childrens::Only(child) => {
                    clear_output_schema_buf(child);
                }
                Childrens::Twins { left, right } => {
                    clear_output_schema_buf(left);
                    clear_output_schema_buf(right);
                }
                Childrens::None => (),
            }
        }

        let table_state = build_t1_table()?;
        let mut plan = table_state.plan("select * from t1 left join t2 on c1 = c3")?;
        clear_output_schema_buf(&mut plan);

        let graph = HepGraph::new(plan.clone());

        let plan_for_graph = graph.into_plan(None).unwrap();

        assert_eq!(plan, plan_for_graph);

        Ok(())
    }
}
