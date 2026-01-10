// Copyright 2024 KipData/KiteSQL
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use rand::rngs::ThreadRng;
use rand::Rng;

pub(crate) struct SeqGen {
    no: usize,
    py: usize,
    os: usize,
    dl: usize,
    sl: usize,
    total: usize,
    seq: Vec<usize>,
    next_num: usize,
    rng: ThreadRng,
}

impl SeqGen {
    pub(crate) fn new(n: usize, p: usize, o: usize, d: usize, s: usize) -> Self {
        let total = n + p + o + d + s;

        Self {
            no: n,
            py: p,
            os: o,
            dl: d,
            sl: s,
            total,
            seq: vec![0; total],
            next_num: 0,
            rng: Default::default(),
        }
    }

    pub(crate) fn get(&mut self) -> usize {
        if self.next_num >= self.total {
            self.shuffle();
            self.next_num = 0;
        }
        let pos = self.next_num;
        self.next_num += 1;
        self.seq[pos]
    }

    pub(crate) fn shuffle(&mut self) {
        let mut pos = 0;

        let mut fn_init = |len, tx| {
            for i in 0..len {
                self.seq[pos + i] = tx;
            }
            pos += len;
        };
        fn_init(self.no, 0);
        fn_init(self.py, 1);
        fn_init(self.os, 2);
        fn_init(self.dl, 3);
        fn_init(self.sl, 4);

        let mut i = 0;
        for j in (0..self.total).rev() {
            let rmd = self.rng.gen::<usize>() % (j + 1);
            let tmp = self.seq[rmd + i];
            self.seq[rmd + i] = self.seq[i];
            self.seq[i] = tmp;

            i += 1;
        }
    }
}
