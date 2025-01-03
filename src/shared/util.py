from itertools import islice


def yield_per(iterable, batch_size):
    """
    Batch any iterable into chunks using islice with a for-loop pattern.

    Args:
        iterable: Any iterable (e.g., generator).
        batch_size: Number of items per batch.

    Yields:
        Lists of size `batch_size` (or smaller for the final batch).
    """
    iterator = iter(iterable)
    for batch in iter(lambda: list(islice(iterator, batch_size)), []):
        yield batch
