from mbf_externals.kent import LiftOver, BedToBigBed


class TestLiftOver:
    def test_fetch_and_run(self, new_pipegraph, per_run_store):
        lift = LiftOver('_latest')
        assert lift.version == "0.1"
        # fetch is done by __init__
        per_run_store.unpack_version(lift.name, lift.version)
        assert (lift.path / 'liftOver').exists()


class TestBedToBigBed:
    def test_fetch_and_run(self, new_pipegraph, per_run_store):
        lift = BedToBigBed('_latest')
        # fetch is done by __init__
        per_run_store.unpack_version(lift.name, lift.version)
        assert (lift.path / 'bedToBigBed').exists()
