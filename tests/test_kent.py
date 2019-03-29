from mbf_externals.kent import LiftOver


class TestLiftOver:
    def test_fetch_and_run(self, new_pipegraph, global_store):
        lift = LiftOver()
        lift.fetch_latest_version()
