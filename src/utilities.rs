pub fn spread<Item, Iterable, Visitor, Value>(
    iterable: Iterable,
    value: Value,
    visitor: Visitor,
) where
    Value: Clone,
    Iterable: IntoIterator<Item = Item>,
    Visitor: Fn(Item, Value),
{
    let mut iterator = iterable.into_iter();
    if let Some(mut next) = iterator.next() {
        for following in iterator {
            visitor(next, value.clone());
            next = following;
        }
        visitor(next, value);
    }
}

pub fn sorted<'a, I, T>(ids: I) -> Vec<T>
where
    I: IntoIterator<Item = &'a T>,
    T: 'a + Copy + Ord,
{
    let mut ids = ids.into_iter().copied().collect::<Vec<_>>();
    ids.sort_unstable();
    ids
}
