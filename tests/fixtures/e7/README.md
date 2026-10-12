# Recorded E7 output

The mock E7 (`app/nbi/mock_e7.py`) was written from the same guesses as the parsers, so tests against it only prove that the two agree. These recordings are what proves the parsers right.

Put the files written by `python -m app.tools.e7_capture --out ...` here, one directory per E7 software release:

    tests/fixtures/e7/R3.1.2/01-show-ont.txt
    tests/fixtures/e7/R3.1.2/05-show-ont-1204-detail.txt
    ...

Each file starts with `$ <command>` and then the E7's raw output. `tests/test_recorded_output.py` runs every recording through the backend's parsers and fails if one cannot be read. A release with no directory here has not been checked: do not write to E7s running it.

Remove passwords, customer names and anything else that should not be in git before committing a capture.
