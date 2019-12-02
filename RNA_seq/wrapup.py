import luigi
from .preprocess import MapFigure, CleanCounts
from luigi.util import inherits


@inherits(CleanCounts)
class AllReports(luigi.WrapperTask):
    def requires(self):
        yield self.clone(CleanCounts)
        yield self.clone(MapFigure)
