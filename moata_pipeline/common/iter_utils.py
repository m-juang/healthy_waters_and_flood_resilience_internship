from __future__ import annotations

from typing import Any, List, TypeVar

T = TypeVar("T")


def chunk(items: List[T], size: int) -> List[List[T]]:
    """
    Split a list into chunks of given size.
    
    Args:
        items: List to split
        size: Maximum size of each chunk
        
    Returns:
        List of chunks
        
    Example:
        >>> chunk([1, 2, 3, 4, 5], 2)
        [[1, 2], [3, 4], [5]]
    """
    if size <= 0:
        raise ValueError("Chunk size must be positive")
    return [items[i : i + size] for i in range(0, len(items), size)]