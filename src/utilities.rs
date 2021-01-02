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
