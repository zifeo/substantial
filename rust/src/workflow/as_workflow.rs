//! What a workflow is passed as: to `start` one, to a worker several.

use super::{Untyped, Workflow};

/// What names a workflow where one is passed: a [`Workflow`], or a function `#[workflow]` declares.
pub trait AsWorkflow {
    type Output;

    fn workflow(&self) -> Workflow<Self::Output>;
}

impl<O> AsWorkflow for Workflow<O> {
    type Output = O;

    fn workflow(&self) -> Workflow<O> {
        self.clone()
    }
}

impl<W: AsWorkflow + ?Sized> AsWorkflow for &W {
    type Output = W::Output;

    fn workflow(&self) -> Workflow<W::Output> {
        (**self).workflow()
    }
}

/// The workflows given to a worker: one, a tuple of them, or a `Vec`.
pub trait IntoWorkflows {
    fn into_workflows(self) -> Vec<Untyped>;
}

impl<W: AsWorkflow> IntoWorkflows for W {
    fn into_workflows(self) -> Vec<Untyped> {
        vec![self.workflow().untyped()]
    }
}

impl<W: AsWorkflow> IntoWorkflows for Vec<W> {
    fn into_workflows(self) -> Vec<Untyped> {
        self.iter().map(|w| w.workflow().untyped()).collect()
    }
}

macro_rules! tuple_workflows {
    ($($w:ident),+) => {
        impl<$($w: AsWorkflow),+> IntoWorkflows for ($($w,)+) {
            #[allow(non_snake_case)]
            fn into_workflows(self) -> Vec<Untyped> {
                let ($($w,)+) = self;
                vec![$($w.workflow().untyped()),+]
            }
        }
    };
}

tuple_workflows!(A);
tuple_workflows!(A, B);
tuple_workflows!(A, B, C);
tuple_workflows!(A, B, C, D);
tuple_workflows!(A, B, C, D, E);
tuple_workflows!(A, B, C, D, E, F);
tuple_workflows!(A, B, C, D, E, F, G);
tuple_workflows!(A, B, C, D, E, F, G, H);
