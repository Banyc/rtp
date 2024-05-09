// pub trait SelfAssignExt: Sized {
//     fn self_assign(&mut self, assign: impl Fn(Self) -> Self) {
//         let placeholder = core::mem::MaybeUninit::uninit();
//         let orig = core::mem::replace(self, unsafe { placeholder.assume_init() });
//         let new = assign(orig);
//         let _ = core::mem::replace(self, new);
//     }
// }
// impl<T> SelfAssignExt for T {}
