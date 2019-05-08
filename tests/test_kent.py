from mbf_externals.kent import LiftOver, BedToBigBed


class TestLiftOver:
    def test_fetch_and_run(self, new_pipegraph, local_store):
        lift = LiftOver()
        lift.fetch_latest_version()
        local_store.unpack_version(lift.name, lift.version)
        assert (lift.path / 'liftOver').exists()


class TestBedToBigBed:
    def test_fetch_and_run(self, new_pipegraph, local_store):
        lift = BedToBigBed()
        lift.fetch_latest_version()
        local_store.unpack_version(lift.name, lift.version)
        assert (lift.path / 'bedToBigBed').exists()
