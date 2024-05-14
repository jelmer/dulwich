import sys
from io import BytesIO

import atheris

with atheris.instrument_imports():
    # We instrument `test_utils` as well, so it doesn't block coverage analysis in Fuzz Introspector:
    from test_utils import EnhancedFuzzedDataProvider, is_expected_exception

    from dulwich.bundle import Bundle, read_bundle, write_bundle
    from dulwich.pack import PackData, write_pack_objects


def TestOneInput(data):
    fdp = EnhancedFuzzedDataProvider(data)
    bundle = Bundle()
    bundle.version = fdp.PickValueInList([2, 3, None])
    bundle.references = {fdp.ConsumeRandomString(): fdp.ConsumeBytes(20)}
    bundle.prerequisites = [(fdp.ConsumeBytes(20), fdp.ConsumeRandomBytes())]
    bundle.capabilities = {
        fdp.ConsumeRandomString(): fdp.ConsumeRandomString(),
    }

    b = BytesIO()
    write_pack_objects(b.write, [])
    b.seek(0)
    bundle.pack_data = PackData.from_file(b)

    # Test __repr__ method
    _ = repr(bundle)

    try:
        bundle_file = BytesIO()
        write_bundle(bundle_file, bundle)
        _ = read_bundle(bundle_file)
    except (AttributeError, UnicodeEncodeError, AssertionError) as e:
        expected_exceptions = [
            "'bytes' object has no attribute 'encode'",
            "surrogates not allowed",
            "unsupported bundle format header",
        ]
        if is_expected_exception(expected_exceptions, e):
            return -1
        else:
            raise e


def main():
    atheris.Setup(sys.argv, TestOneInput)
    atheris.Fuzz()


if __name__ == "__main__":
    main()
