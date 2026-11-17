#!/usr/bin/env python3
"""Encode/decode interop matrix for OSC bundles over stream framings.

Builds a fixed set of OSC bundles (non-ASCII arguments, NTP timetags and
nested bundles), frames them with every stream framing, feeds the bytes to a
FramingParser in chunks of various sizes, and verifies that the decoded
bundles round-trip value for value. Also pipes the framed stream through a
separate decoder process to prove cross-process interop.

For every matrix cell the timetag round-trip deviation (microseconds) and the
SHA-256 content hash are printed. A final section checks that invalid inputs
raise stable exception types.

Run with:  uv run python scripts/bundle_interop_matrix.py
Exit code is 0 only if every check passes.
"""

import hashlib
import json
import subprocess
import sys
from pathlib import Path

# Make the repo importable both here and in the decoder child process.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pythonosc import osc_bundle, osc_bundle_builder, osc_message, osc_message_builder
from pythonosc.parsing import framing, ntp, osc_types

# Fixed timestamps (no wall clock anywhere, output is deterministic).
TS_MAIN = 1767225600.167749  # 2026-01-01T00:00:00.167749 UTC, non-binary-exact
TS_INNER = 1767225601.5  # binary-exact fraction
IMMEDIATELY = osc_types.IMMEDIATELY

FRAMINGS = {"slip": framing.SLIP_FRAMING, "length": framing.BINARY_LENGTH_FRAMING}
CHUNK_SIZES = [1, 2, 3, 5, 7, 16, 64, None]  # None = single feed

FAILURES = []


def check(ok: bool, label: str, detail: str = "") -> None:
    if not ok:
        FAILURES.append(label)
        print(f"  FAIL {label} {detail}")


def build_message(address: str, args: list) -> osc_message.OscMessage:
    builder = osc_message_builder.OscMessageBuilder(address=address)
    for value in args:
        builder.add_arg(value)
    return builder.build()


def timetag_message(address: str, system_time: float) -> osc_message.OscMessage:
    """A message with a raw 't' OSC timetag argument (unsupported by the builder)."""
    dgram = (
        osc_types.write_string(address)
        + osc_types.write_string(",t")
        + ntp.system_time_to_ntp(system_time)
    )
    return osc_message.OscMessage(dgram)


def make_bundle(timestamp, contents) -> osc_bundle.OscBundle:
    builder = osc_bundle_builder.OscBundleBuilder(timestamp)
    for content in contents:
        builder.add_content(content)
    return builder.build()


def fixtures() -> dict:
    """Fixed bundles; each entry is (bundle, source timestamps in flatten order)."""
    non_ascii = make_bundle(
        IMMEDIATELY,
        [
            build_message(
                "/ctrl/名前",
                [
                    "héllo wörld",
                    "日本語テスト",
                    "🎛️",
                    "バイナリ".encode("utf-8") + b"\x00\xff",
                    7,
                    0.5,
                    0.1,
                    True,
                    None,
                ],
            )
        ],
    )
    timetag = make_bundle(
        TS_MAIN,
        [
            timetag_message("/clock/sync", TS_MAIN + 0.25),
            build_message("/clock/drift", [0.000001]),
        ],
    )
    nested = make_bundle(
        TS_INNER,
        [
            build_message("/mixer/volume", [0.8, "主音量"]),
            make_bundle(
                TS_MAIN,
                [
                    build_message("/灯/亮度", ["暖かい", 3000]),
                    make_bundle(IMMEDIATELY, [build_message("/ping", [])]),
                ],
            ),
        ],
    )
    return {
        "non-ascii": (non_ascii, [IMMEDIATELY]),
        "timetag": (timetag, [TS_MAIN]),
        "nested": (nested, [TS_INNER, TS_MAIN, IMMEDIATELY]),
    }


def flatten(bundle: osc_bundle.OscBundle) -> list:
    """Flatten a bundle to comparable (kind, ...) entries, depth first."""
    out = [("bundle", bundle.timestamp)]
    for content in bundle:
        if isinstance(content, osc_bundle.OscBundle):
            out.extend(flatten(content))
        else:
            out.append(("message", content.address, tuple(content.params)))
    return out


def feed_chunked(frm: framing.Framing, stream: bytes, chunk_size):
    parser = framing.FramingParser(frm)
    dgrams = []
    if chunk_size is None:
        dgrams.extend(parser.feed(stream))
    else:
        for i in range(0, len(stream), chunk_size):
            dgrams.extend(parser.feed(stream[i : i + chunk_size]))
    return dgrams, parser.remaining


def run_matrix(fx: dict) -> None:
    print("== encode/decode matrix (in-process, chunked FramingParser) ==")
    print(
        f"{'bundle':<10} {'framing':<8} {'chunk':>5} {'dgrams':>6} {'bytes':>6} "
        f"{'Δt µs':>10} {'values':>6}  sha256"
    )
    for name, (bundle, source_ts) in fx.items():
        ref_flat = flatten(bundle)
        ref_hash = hashlib.sha256(bundle.dgram).hexdigest()
        for fname, frm in FRAMINGS.items():
            stream = frm.encode(bundle.dgram)
            for chunk in CHUNK_SIZES:
                label = f"{name}/{fname}/chunk={chunk}"
                dgrams, remaining = feed_chunked(frm, stream, chunk)
                ok = remaining == b"" and dgrams == [bundle.dgram]
                check(ok, label, "stream reassembly mismatch")
                parsed = osc_bundle.OscBundle(dgrams[0]) if dgrams else None
                values_ok = parsed is not None and flatten(parsed) == ref_flat
                check(values_ok, label, "value mismatch after round trip")
                # Timetag deviation vs the source system-time values.
                parsed_ts = [e[1] for e in flatten(parsed) if e[0] == "bundle"]
                dt_us = max(abs(p - s) * 1e6 for p, s in zip(parsed_ts, source_ts))
                digest = hashlib.sha256(dgrams[0]).hexdigest()
                check(digest == ref_hash, label, "hash drifted between chunk sizes")
                print(
                    f"{name:<10} {fname:<8} {str(chunk):>5} {len(dgrams):>6} "
                    f"{len(stream):>6} {dt_us:>10.6f} "
                    f"{'OK' if values_ok else 'DIFF':>6}  {digest[:16]}"
                )
    print()


def jsonable(value):
    if isinstance(value, bytes):
        return {"__blob__": value.hex()}
    if isinstance(value, tuple):
        return [jsonable(v) for v in value]
    if isinstance(value, list):
        return [jsonable(v) for v in value]
    if hasattr(value, "isoformat"):  # datetime from 't' timetag arguments
        return {"__datetime__": value.isoformat()}
    return value


def flat_to_json(flat: list) -> list:
    return [
        ["bundle", entry[1]]
        if entry[0] == "bundle"
        else ["message", entry[1], jsonable(list(entry[2]))]
        for entry in flat
    ]


def child_main() -> int:
    """Decoder process: read a framed stream from stdin, dump parsed bundles."""
    frm = FRAMINGS[sys.argv[2]]
    parser = framing.FramingParser(frm)
    dgrams = []
    while chunk := sys.stdin.buffer.read(4096):
        dgrams.extend(parser.feed(chunk))
    if parser.remaining:
        print(f"trailing undecoded bytes: {parser.remaining!r}", file=sys.stderr)
        return 2
    for dgram in dgrams:
        bundle = osc_bundle.OscBundle(dgram)
        record = {
            "sha256": hashlib.sha256(dgram).hexdigest(),
            "flat": flat_to_json(flatten(bundle)),
        }
        print(json.dumps(record), flush=True)
    return 0


def run_cross_process(fx: dict) -> None:
    print("== cross-process round trip (encoder here, decoder subprocess) ==")
    for fname, frm in FRAMINGS.items():
        stream = b"".join(frm.encode(bundle.dgram) for bundle, _ in fx.values())
        proc = subprocess.Popen(
            [sys.executable, __file__, "--child", fname],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
        )
        # Write in awkward 5-byte chunks to stress chunked reassembly.
        for i in range(0, len(stream), 5):
            proc.stdin.write(stream[i : i + 5])
            proc.stdin.flush()
        proc.stdin.close()
        records = [json.loads(line) for line in proc.stdout]
        proc.wait()
        check(proc.returncode == 0, f"xproc/{fname}", f"rc={proc.returncode}")
        check(len(records) == len(fx), f"xproc/{fname}", "bundle count mismatch")
        for (name, (bundle, _)), record in zip(fx.items(), records):
            hash_ok = record["sha256"] == hashlib.sha256(bundle.dgram).hexdigest()
            values_ok = record["flat"] == flat_to_json(flatten(bundle))
            check(hash_ok and values_ok, f"xproc/{fname}/{name}")
            print(
                f"{fname:<8} {name:<10} hash={'OK' if hash_ok else 'DIFF'} "
                f"values={'OK' if values_ok else 'DIFF'} {record['sha256'][:16]}"
            )
    print()


def expect_raise(label: str, exc_type, fn) -> None:
    try:
        fn()
    except exc_type as e:
        print(f"  PASS {label:<38} -> {type(e).__name__}: {e}")
    except Exception as e:  # noqa: BLE001 - report the unexpected type
        check(False, label, f"raised {type(e).__name__} instead of {exc_type.__name__}")
        print(f"  FAIL {label:<38} -> {type(e).__name__} (wanted {exc_type.__name__})")
    else:
        check(False, label, "no exception raised")
        print(f"  FAIL {label:<38} -> no exception (wanted {exc_type.__name__})")


def run_invalid_inputs(fx: dict) -> None:
    print("== invalid inputs raise stable exceptions ==")
    nested_dgram = fx["nested"][0].dgram
    prefix = b"#bundle\x00" + ntp.IMMEDIATELY

    expect_raise(
        "truncated bundle",
        osc_bundle.ParseError,
        lambda: osc_bundle.OscBundle(nested_dgram[:-3]),
    )
    expect_raise(
        "negative element size",
        osc_bundle.ParseError,
        lambda: osc_bundle.OscBundle(prefix + osc_types.write_int(-4) + b"x" * 4),
    )
    expect_raise(
        "element size beyond datagram",
        osc_bundle.ParseError,
        lambda: osc_bundle.OscBundle(prefix + osc_types.write_size(64) + b"x" * 8),
    )
    expect_raise(
        "element size not multiple of 4",
        osc_bundle.ParseError,
        lambda: osc_bundle.OscBundle(prefix + osc_types.write_size(5) + b"x" * 8),
    )
    expect_raise(
        "invalid utf-8 string argument",
        osc_message.ParseError,
        lambda: osc_message.OscMessage(
            osc_types.write_string("/a")
            + osc_types.write_string(",s")
            + b"\xff\xfe\x00\x00"
        ),
    )
    expect_raise(
        "blob length overruns datagram",
        osc_message.ParseError,
        lambda: osc_message.OscMessage(
            osc_types.write_string("/a")
            + osc_types.write_string(",b")
            + osc_types.write_int(8)
            + b"\x00" * 4
        ),
    )
    expect_raise(
        "negative size prefix on stream",
        osc_types.ParseError,
        lambda: framing.FramingParser(framing.BINARY_LENGTH_FRAMING).feed(
            b"\xff\xff\xff\xff"
        ),
    )
    expect_raise(
        "truncated timetag",
        osc_types.ParseError,
        lambda: osc_types.get_date(b"\x00" * 4, 0),
    )
    expect_raise(
        "negative size prefix encode",
        osc_types.BuildError,
        lambda: osc_types.write_size(-1),
    )
    expect_raise(
        "non-numeric system time",
        ntp.NtpError,
        lambda: ntp.system_time_to_ntp("not-a-number"),
    )
    print()


def main() -> int:
    if len(sys.argv) == 3 and sys.argv[1] == "--child":
        return child_main()

    fx = fixtures()
    run_matrix(fx)
    run_cross_process(fx)
    run_invalid_inputs(fx)

    if FAILURES:
        print(f"{len(FAILURES)} check(s) FAILED")
        return 1
    print("all checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
