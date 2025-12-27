"""
Text Utilities Module

Provides utilities for text processing, formatting, and sanitization.

Functions:
    safe_filename: Convert text to safe filename
    truncate_text: Truncate text with ellipsis
    normalize_whitespace: Normalize whitespace in text
    slugify: Convert text to URL-safe slug
    strip_html_tags: Remove HTML tags from text
    format_number: Format number with commas
    pluralize: Add plural suffix based on count

Author: Auckland Council Internship Team (COMPSCI 778)
Last Modified: 2024-12-28
Version: 1.0.0
"""

from __future__ import annotations

import re
from typing import Optional


# Version info
__version__ = "1.0.0"


# =============================================================================
# Text Sanitization Functions
# =============================================================================

def safe_filename(name: str, max_len: int = 120, replacement: str = "_") -> str:
    """
    Convert text to safe filename by removing/replacing invalid characters.
    
    Removes characters that are invalid in filenames on most operating systems.
    Replaces spaces with underscores (or custom replacement).
    
    Args:
        name: Text to convert to filename
        max_len: Maximum filename length (default: 120)
        replacement: Character to replace invalid chars (default: "_")
        
    Returns:
        Safe filename string
        
    Example:
        >>> safe_filename("My File: Data (2024).txt")
        'My_File_Data_2024_.txt'
        
        >>> safe_filename("Report #1 / Analysis", max_len=20)
        'Report_1_Analysis'
    """
    if not name:
        return "untitled"
    
    # Strip whitespace
    name = name.strip()
    
    # Replace invalid characters with replacement
    # Keep alphanumeric, spaces, hyphens, dots, underscores
    name = re.sub(r"[^\w\s\-\.]", replacement, name)
    
    # Replace multiple spaces/underscores with single replacement
    name = re.sub(r"\s+", replacement, name)
    name = re.sub(f"{re.escape(replacement)}+", replacement, name)
    
    # Remove leading/trailing replacement characters
    name = name.strip(replacement)
    
    # Truncate to max length
    if len(name) > max_len:
        # Try to preserve file extension if present
        if "." in name:
            parts = name.rsplit(".", 1)
            if len(parts[1]) <= 10:  # Reasonable extension length
                base = parts[0][:max_len - len(parts[1]) - 1]
                name = f"{base}.{parts[1]}"
            else:
                name = name[:max_len]
        else:
            name = name[:max_len]
    
    # Ensure we have something
    return name if name else "untitled"


def truncate_text(
    text: str,
    max_length: int = 100,
    suffix: str = "...",
    word_boundary: bool = True,
) -> str:
    """
    Truncate text to maximum length with ellipsis.
    
    Args:
        text: Text to truncate
        max_length: Maximum length including suffix (default: 100)
        suffix: Suffix to add when truncated (default: "...")
        word_boundary: Truncate at word boundary (default: True)
        
    Returns:
        Truncated text
        
    Example:
        >>> truncate_text("This is a very long text that needs truncating", 20)
        'This is a very...'
        
        >>> truncate_text("Short text", 20)
        'Short text'
    """
    if not text or len(text) <= max_length:
        return text
    
    # Account for suffix length
    truncate_at = max_length - len(suffix)
    
    if truncate_at <= 0:
        return suffix[:max_length]
    
    truncated = text[:truncate_at]
    
    # Find last word boundary if requested
    if word_boundary:
        last_space = truncated.rfind(" ")
        if last_space > 0:
            truncated = truncated[:last_space]
    
    return truncated + suffix


def normalize_whitespace(text: str) -> str:
    """
    Normalize whitespace in text.
    
    Replaces multiple spaces, tabs, newlines with single space.
    Strips leading/trailing whitespace.
    
    Args:
        text: Text to normalize
        
    Returns:
        Normalized text
        
    Example:
        >>> normalize_whitespace("  Hello   \\n\\t  World  ")
        'Hello World'
    """
    if not text:
        return ""
    
    # Replace all whitespace with single space
    normalized = re.sub(r"\s+", " ", text)
    
    # Strip leading/trailing whitespace
    return normalized.strip()


def slugify(text: str, max_length: int = 50) -> str:
    """
    Convert text to URL-safe slug.
    
    Converts to lowercase, removes special characters, replaces spaces with hyphens.
    
    Args:
        text: Text to convert to slug
        max_length: Maximum slug length (default: 50)
        
    Returns:
        URL-safe slug
        
    Example:
        >>> slugify("Hello World! This is a Test")
        'hello-world-this-is-a-test'
        
        >>> slugify("Auckland Council: Rain Gauges (2024)")
        'auckland-council-rain-gauges-2024'
    """
    if not text:
        return ""
    
    # Convert to lowercase
    slug = text.lower()
    
    # Replace spaces and underscores with hyphens
    slug = re.sub(r"[\s_]+", "-", slug)
    
    # Remove non-alphanumeric characters (except hyphens)
    slug = re.sub(r"[^a-z0-9\-]", "", slug)
    
    # Replace multiple hyphens with single hyphen
    slug = re.sub(r"\-+", "-", slug)
    
    # Remove leading/trailing hyphens
    slug = slug.strip("-")
    
    # Truncate to max length
    if len(slug) > max_length:
        slug = slug[:max_length].rstrip("-")
    
    return slug


def strip_html_tags(text: str) -> str:
    """
    Remove HTML tags from text.
    
    Args:
        text: Text potentially containing HTML tags
        
    Returns:
        Text with HTML tags removed
        
    Example:
        >>> strip_html_tags("<p>Hello <strong>World</strong></p>")
        'Hello World'
    """
    if not text:
        return ""
    
    # Remove HTML tags
    clean = re.sub(r"<[^>]+>", "", text)
    
    # Normalize whitespace after tag removal
    return normalize_whitespace(clean)


# =============================================================================
# Text Formatting Functions
# =============================================================================

def format_number(
    number: float,
    decimals: int = 2,
    thousands_sep: str = ",",
) -> str:
    """
    Format number with thousands separator and decimals.
    
    Args:
        number: Number to format
        decimals: Number of decimal places (default: 2)
        thousands_sep: Thousands separator (default: ",")
        
    Returns:
        Formatted number string
        
    Example:
        >>> format_number(1234567.89)
        '1,234,567.89'
        
        >>> format_number(1234567.89, decimals=0)
        '1,234,568'
    """
    try:
        # Round to specified decimals
        rounded = round(float(number), decimals)
        
        # Format with thousands separator
        if decimals == 0:
            return f"{int(rounded):,}".replace(",", thousands_sep)
        else:
            formatted = f"{rounded:,.{decimals}f}"
            return formatted.replace(",", thousands_sep)
    except (ValueError, TypeError):
        return str(number)


def pluralize(
    word: str,
    count: int,
    plural_form: Optional[str] = None,
) -> str:
    """
    Add plural suffix based on count.
    
    Args:
        word: Singular form of word
        count: Count to determine singular/plural
        plural_form: Optional custom plural form (default: add 's')
        
    Returns:
        Singular or plural form based on count
        
    Example:
        >>> pluralize("gauge", 1)
        'gauge'
        
        >>> pluralize("gauge", 5)
        'gauges'
        
        >>> pluralize("catchment", 3, "catchments")
        'catchments'
    """
    if count == 1:
        return word
    
    if plural_form:
        return plural_form
    
    # Simple pluralization rules
    if word.endswith(("s", "x", "z", "ch", "sh")):
        return word + "es"
    elif word.endswith("y") and len(word) > 1 and word[-2] not in "aeiou":
        return word[:-1] + "ies"
    else:
        return word + "s"


def format_list(
    items: list,
    conjunction: str = "and",
    oxford_comma: bool = True,
) -> str:
    """
    Format list of items as grammatical string.
    
    Args:
        items: List of items to format
        conjunction: Conjunction word (default: "and")
        oxford_comma: Use Oxford comma (default: True)
        
    Returns:
        Formatted string
        
    Example:
        >>> format_list(["A", "B", "C"])
        'A, B, and C'
        
        >>> format_list(["A", "B"], conjunction="or")
        'A or B'
    """
    if not items:
        return ""
    
    items = [str(item) for item in items]
    
    if len(items) == 1:
        return items[0]
    elif len(items) == 2:
        return f"{items[0]} {conjunction} {items[1]}"
    else:
        comma = "," if oxford_comma else ""
        return f"{', '.join(items[:-1])}{comma} {conjunction} {items[-1]}"


def pad_string(
    text: str,
    width: int,
    align: str = "left",
    fill_char: str = " ",
) -> str:
    """
    Pad string to specified width.
    
    Args:
        text: Text to pad
        width: Target width
        align: Alignment ('left', 'right', 'center')
        fill_char: Character to use for padding (default: space)
        
    Returns:
        Padded string
        
    Example:
        >>> pad_string("Hello", 10, align="right")
        '     Hello'
        
        >>> pad_string("Test", 10, align="center", fill_char="*")
        '***Test***'
    """
    if len(text) >= width:
        return text
    
    if align == "right":
        return text.rjust(width, fill_char)
    elif align == "center":
        return text.center(width, fill_char)
    else:  # left
        return text.ljust(width, fill_char)