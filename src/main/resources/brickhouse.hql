
CREATE TEMPORARY FUNCTION append_array AS 'brickhouse.udf.collect.AppendArrayUDF';
CREATE TEMPORARY FUNCTION array_index AS 'brickhouse.udf.collect.ArrayIndexUDF';
CREATE TEMPORARY FUNCTION intersect_array AS 'brickhouse.udf.collect.ArrayIntersectUDF';
CREATE TEMPORARY FUNCTION collect AS 'brickhouse.udf.collect.CollectUDAF';
CREATE TEMPORARY FUNCTION collect_max AS 'brickhouse.udf.collect.CollectMaxUDAF';
CREATE TEMPORARY FUNCTION combine_unique AS 'brickhouse.udf.collect.CombineUniqueUDAF';
CREATE TEMPORARY FUNCTION join_array AS 'brickhouse.udf.collect.JoinArrayUDF';
CREATE TEMPORARY FUNCTION map_filter_keys AS 'brickhouse.udf.collect.MapFilterKeysUDF';
CREATE TEMPORARY FUNCTION map_key_values AS 'brickhouse.udf.collect.MapKeyValuesUDF';
CREATE TEMPORARY FUNCTION multiday_count AS 'brickhouse.udf.collect.MultiDayCounterUDAF';
CREATE TEMPORARY FUNCTION sum_array AS 'brickhouse.udf.collect.SumArrayUDF';
CREATE TEMPORARY FUNCTION union_max AS 'brickhouse.udf.collect.UnionMaxUDAF';
CREATE TEMPORARY FUNCTION union_map AS 'brickhouse.udf.collect.UnionUDAF';

CREATE TEMPORARY FUNCTION json_map AS 'brickhouse.udf.json.JsonMapUDF';
CREATE TEMPORARY FUNCTION json_split AS 'brickhouse.udf.json.JsonSplitUDF';
CREATE TEMPORARY FUNCTION to_json AS 'brickhouse.udf.json.ToJsonUDF';
CREATE TEMPORARY FUNCTION distributed_map AS 'brickhouse.udf.dcache.DistributedMapUDF';

CREATE TEMPORARY FUNCTION combine_previous_sketch AS 'brickhouse.udf.sketch.CombinePreviousSketchUDF';
CREATE TEMPORARY FUNCTION combine_sketch AS 'brickhouse.udf.sketch.CombineSketchUDF';
CREATE TEMPORARY FUNCTION convert_to_sketch AS 'brickhouse.udf.sketch.ConvertToSketchUDF';
CREATE TEMPORARY FUNCTION estimated_reach AS 'brickhouse.udf.sketch.EstimatedReachUDF';
CREATE TEMPORARY FUNCTION multiday_sketch AS 'brickhouse.udf.sketch.MultiDaySketcherUDAF';
CREATE TEMPORARY FUNCTION sketch_set AS 'brickhouse.udf.sketch.SketchSetUDAF';
CREATE TEMPORARY FUNCTION union_sketch AS 'brickhouse.udf.sketch.UnionSketchSetUDAF';

CREATE TEMPORARY FUNCTION moving_avg AS 'brickhouse.udf.timeseries.MovingAvgUDF';

CREATE TEMPORARY FUNCTION bloom AS 'brickhouse.udf.bloom.BloomUDAF';
CREATE TEMPORARY FUNCTION distributed_bloom AS 'brickhouse.udf.bloom.DistributedBloomUDF';
CREATE TEMPORARY FUNCTION merge_bloom AS 'brickhouse.udf.bloom.MergeBloomUDF';
CREATE TEMPORARY FUNCTION bloom_contains AS 'brickhouse.udf.bloom.BloomContainsUDF';
CREATE TEMPORARY FUNCTION bloom_and AS 'brickhouse.udf.bloom.BloomAndUDF';
CREATE TEMPORARY FUNCTION bloom_or AS 'brickhouse.udf.bloom.BloomOrUDF';
CREATE TEMPORARY FUNCTION bloom_not AS 'brickhouse.udf.bloom.BloomNotUDF';

CREATE TEMPORARY FUNCTION add_days AS 'brickhouse.udf.date.AddDaysUDF';


