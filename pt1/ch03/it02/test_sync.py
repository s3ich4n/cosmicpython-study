from pathlib import Path

from sync import determine_actions


def test_when_a_file_exists_in_the_source_but_not_the_destination():
    src_hashes = {'hash1': 'fn1'}
    dst_hashes = {}

    actions = determine_actions(
        src_hashes,
        dst_hashes,
        Path("/src"),
        Path("/dst"),
    )

    assert list(actions) == [('COPY', Path('/src/fn1'), Path('/dst/fn1'))]


def test_when_a_file_has_been_removed_in_the_source():
    src_hashes = {'hash1': 'fn1'}
    dst_hashes = {'hash1': 'fn2'}

    actions = determine_actions(
        src_hashes,
        dst_hashes,
        Path("/src"),
        Path("/dst"),
    )

    assert list(actions) == [('MOVE', Path('/dst/fn2'), Path('/dst/fn1'))]


def test_when_a_file_exists_in_the_destination_but_not_the_source():
    src_hashes = {}
    dst_hashes = {'hash1': 'fn1'}

    actions = determine_actions(
        src_hashes,
        dst_hashes,
        Path("/src"),
        Path("/dst"),
    )

    assert list(actions) == [('DELETE', Path('/dst/fn1'))]
