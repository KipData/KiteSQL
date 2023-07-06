use std::{any::Any, sync::Arc};

use super::{meta_pipeline::PipelineIx, pipeline_executor::PipelineExecuteResult};
use crate::executor::{
    executor_task::Task, parallel::pipeline_executor::PipelineExecutor,
    physical::PhysicalOperatorRef,
};
use anyhow::Result;

/// Pipeline represent a chain of physical operators that are executed in
/// sequence that include `source`, `operator` and `sink.
///
/// To improve performance. The pipeline can be split into multiple
/// sub-pipelines that are executed in parallel.
///
/// # Define a physical operator whether sink operator.
/// If any operator that need to digest the data of all child nodes before they
/// can proceed to the next step that called **Pipeline Breaker**
///
/// # How to split into multiple sub-pipelines depends on whether physical operator is `Pipeline breaker`.
/// * If a physical operator is `Pipeline breaker` pull it out and use it as new
///   sub pipeline source,
/// * And use it as prev sub pipeline sink.
///
/// For Example:
///
/// SELECT * FROM t1;
/// Pipeline:  TableScan (push to) Project.
///
/// SELECT * FROM t1 GROUP BY a LIMIT 10;
/// Pipeline0:  Table Scan (push to) Project (push to) GROUP BY a.
/// Pipeline1:  (Depends on Pipeline0 GROUP BY a) Project (push to) TOP10.
///
/// SELECT * FROM t1 ORDER BY a LIMIT 10;
/// Pipeline0: Table Scan (push to) Project (push to) ORDER BY a.
/// Pipeline1: (Depends on Pipeline0 ORDER BY a) Project (push to) TOP10.
///
/// SELECT * FROM t1 UNION All SELECT * FROM t2
/// Pipeline0: Table Scan t1(push to) Project
/// Pipeline1: Table Scan t2(push to) Project
/// Pipeline2: (Depends on Pipeline0,Pipeline1 Project) Project.
/// So that Pipeline0 and Pipeline1 not any dependencies so can concurrency
/// execute.
///
/// # How to execution.
/// * Constructor physical operators to pipeline
/// * Pick no dependency pipeline to executed first(pipeline 0).
/// * If pipeline 0 is complete, pick another pipeline that only reply on it to
///   execute.
/// * When all operations in a pipeline support parallelization, the pipeline is
///   executed in parallel.
#[derive(Clone, Debug)]
pub struct Pipeline {
    pub pipeline_id: PipelineIx,

    /// The source of this pipeline.
    pub source: Option<PhysicalOperatorRef>,

    /// THe chain of intermediate operators.
    pub operators: Vec<PhysicalOperatorRef>,

    /// The sink of this pipeline.
    pub sink: Option<PhysicalOperatorRef>,

    /// The parent pipelines.
    pub parents: Vec<PipelineIx>,

    /// The dependencies of this pipeline.
    pub dependencies: Vec<PipelineIx>,

    pub base_batch_index: u64,
}

impl Pipeline {
    pub fn new(pipeline_id: PipelineIx) -> Pipeline {
        Pipeline {
            source: None,
            operators: Vec::new(),
            sink: None,
            parents: Vec::new(),
            dependencies: Vec::new(),
            base_batch_index: 0,
            pipeline_id,
        }
    }

    pub fn get_pipeline_id(&self) -> PipelineIx {
        self.pipeline_id
    }

    pub fn get_dependencies(&self) -> &[PipelineIx] {
        &self.dependencies
    }

    pub fn reset(&self) {}

    pub fn finalize(&self) {
        //     	if (executor.HasError()) {
        // 	return;
        // }
        // D_ASSERT(ready);
        // try {
        // 	auto sink_state = sink->Finalize(*this, event, executor.context,
        // *sink->sink_state); 	sink->sink_state->state = sink_state;
        // } catch (Exception &ex) { // LCOV_EXCL_START
        // 	executor.PushError(PreservedError(ex));
        // } catch (std::exception &ex) {
        // 	executor.PushError(PreservedError(ex));
        // } catch (...) {
        // 	executor.PushError(PreservedError("Unknown exception in Finalize!"));
        // } // L
    }

    pub fn get_source(&self) -> Option<PhysicalOperatorRef> {
        self.source.clone()
    }

    pub fn can_parallel(&self) -> bool {
        true
    }

    pub fn schedule_sequential(&self) -> Vec<Arc<dyn Task>> {
        vec![]
    }

    pub fn schedule_parallel(&self) -> Vec<Arc<dyn Task>> {
        vec![]
    }
}
