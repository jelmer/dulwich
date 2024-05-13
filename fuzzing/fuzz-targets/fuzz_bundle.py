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
    bundle.version = fdp.PickValueInList([2, 3])
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
    except (AttributeError, UnicodeEncodeError) as e:
        expected_exceptions = [
            "'bytes' object has no attribute 'encode'",
            "surrogates not allowed",
        ]
        if is_expected_exception(expected_exceptions, e):
            return
        else:
            raise e

    bundle_file.seek(0)
    _ = read_bundle(bundle_file)

    # Test __eq__ method
    # Create a different bundle for inequality testing _after_ read/write tests.
    # The read/write tests may have consumed all the `data` via the `fdp` "Consume" methods, so we build the second
    # bundle _after_ so those tests can execute even before the fuzzing engine begins providing large enough inputs to
    # populate the second bundle's fields.
    other_bundle = Bundle()
    other_bundle.version = bundle.version
    other_bundle.references = {fdp.ConsumeRandomString(): fdp.ConsumeBytes(20)}
    other_bundle.prerequisites = [(fdp.ConsumeBytes(20), fdp.ConsumeRandomBytes())]
    other_bundle.capabilities = {
        fdp.ConsumeRandomString(): fdp.ConsumeRandomString(),
    }
    b2 = BytesIO()
    write_pack_objects(b2.write, [])
    b2.seek(0)
    other_bundle.pack_data = PackData.from_file(b2)
    _ = bundle != other_bundle


def main():
    atheris.Setup(sys.argv, TestOneInput)
    atheris.Fuzz()


if __name__ == "__main__":
    main()
