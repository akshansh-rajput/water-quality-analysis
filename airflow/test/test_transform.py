from pyspark.sql import Row
import pytest
from pyspark.sql import functions as F
from airflow.dags.deployment.data_handler.transformations.transformation import  (filter_data,
                                                                                  convert_to_json,
                                                                                  remove_cols,
                                                                                  selected_cols,
                                                                                  select_expr,
                                                                                  extract_from_json,
                                                                                  inner_join,
                                                                                  custom_expr)

@pytest.fixture(scope="session")
def sample_data(spark):
    data = [
        {"id": "sam_1", "sample_value": 200},
        {"id": "sam_2", "sample_value": 600},
        {"id": "sam_3", "sample_value": 750},
    ]
    df = spark.createDataFrame(Row(**x) for x in data)
    return df


# Filter transformation test
def test_filter_data(spark, sample_data):
    expected_data = [
        {"id": "sam_2", "sample_value": 600},
        {"id": "sam_3", "sample_value": 750},
    ]
    expected_rows = [Row(**x) for x in expected_data]
    condition = "sample_value > 500"
    result = filter_data(sample_data, condition)
    assert result.rdd.collect() == expected_rows

def test_filter_data_neg(spark, sample_data):
    expected_data = [
        {"id": "sam_2", "sample_value": 600},
        {"id": "sam_3", "sample_value": 750},
    ]
    expected_rows = [Row(**x) for x in expected_data]
    condition = "sample_value_extra > 500"
    with pytest.raises(Exception):
        filter_data(sample_data, condition)

# to_json transformation test
def test_to_json_data(spark, sample_data):
    expected_data = [
        {"value":"{\"id\":\"sam_1\",\"sample_value\":200}"},
        {"value":"{\"id\":\"sam_2\",\"sample_value\":600}"},
        {"value":"{\"id\":\"sam_3\",\"sample_value\":750}"},
    ]
    expected_rows = [Row(**x) for x in expected_data]
    col_name = "value"
    result = convert_to_json(sample_data, col_name)
    assert result.rdd.collect() == expected_rows

# remove_cols transformation test
def test_remove_cols_data(spark, sample_data):
    expected_data = [
        {"id": "sam_1"},
        {"id": "sam_2"},
        {"id": "sam_3"},
    ]
    expected_rows = [Row(**x) for x in expected_data]
    condition = "sample_value"
    result = remove_cols(sample_data, condition)
    assert result.rdd.collect() == expected_rows

def test_remove_cols_data_neg(spark, sample_data):
    expected_data = [
        {"id": "sam_1"},
        {"id": "sam_2"},
        {"id": "sam_3"},
    ]
    expected_rows = [Row(**x) for x in expected_data]
    condition = "sample_value_extra"
    with pytest.raises(Exception):
        filter_data(sample_data, condition)

# selected_cols transformation test
def test_selected_cols_data(spark, sample_data):
    expected_data = [
        {"id": "sam_1"},
        {"id": "sam_2"},
        {"id": "sam_3"},
    ]
    expected_rows = [Row(**x) for x in expected_data]
    condition = "id"
    result = selected_cols(sample_data, condition)
    assert result.rdd.collect() == expected_rows

def test_selected_cols_data_neg(spark, sample_data):
    expected_data = [
        {"id": "sam_1"},
        {"id": "sam_2"},
        {"id": "sam_3"},
    ]
    expected_rows = [Row(**x) for x in expected_data]
    condition = "extra_id"
    with pytest.raises(Exception):
        selected_cols(sample_data, condition)

# select_expr transformation test
def test_select_expr_data(spark, sample_data):
    expected_data = [
        {"id": "sam_1200"},
        {"id": "sam_2600"},
        {"id": "sam_3750"},
    ]
    expected_rows = [Row(**x) for x in expected_data]
    condition = "concat(id,sample_value) as id"
    result = select_expr(sample_data, condition)
    assert result.rdd.collect() == expected_rows

def test_select_expr_data_neg(spark, sample_data):
    expected_data = [
        {"id": "sam_1200"},
        {"id": "sam_2600"},
        {"id": "sam_3750"},
    ]
    expected_rows = [Row(**x) for x in expected_data]
    condition = "concat(id_1,sample_value) as id"
    with pytest.raises(Exception):
        select_expr(sample_data, condition)

# extract_from_json transformation test
def test_extract_from_json_data(spark, sample_data):
    expected_data = [
        {"id": "sam_1", "sample_value": 200},
        {"id": "sam_2", "sample_value": 600},
        {"id": "sam_3", "sample_value": 750},
    ]
    sch = sample_data.schema
    input_data = convert_to_json(sample_data, 'value')
    expected_rows = [Row(**x) for x in expected_data]
    result = extract_from_json(input_data, sch)
    assert result.rdd.collect() == expected_rows

def test_extract_from_json_data_neg(spark, sample_data):
    expected_data = [
        {"id": "sam_1", "sample_value": 200},
        {"id": "sam_2", "sample_value": 600},
        {"id": "sam_3", "sample_value": 750},
    ]
    sch = sample_data.drop("id").schema
    input_data = convert_to_json(sample_data, 'value')
    expected_rows = [Row(**x) for x in expected_data]
    result = extract_from_json(input_data, sch)
    assert not result.rdd.collect() == expected_rows

# # Filter transformation test
# def test_filter_data(spark, sample_data):
#     expected_data = [
#         {"id": "sam_2", "sample_value": 600},
#         {"id": "sam_3", "sample_value": 750},
#     ]
#     expected_rows = [Row(**x) for x in expected_data]
#     condition = "sample_value > 500"
#     result = filter_data(sample_data, condition)
#     assert result.rdd.collect() == expected_rows

# def test_filter_data_neg(spark, sample_data):
#     expected_data = [
#         {"id": "sam_2", "sample_value": 600},
#         {"id": "sam_3", "sample_value": 750},
#     ]
#     expected_rows = [Row(**x) for x in expected_data]
#     condition = "sample_value_extra > 500"
#     with pytest.raises(Exception):
#         filter_data(sample_data, condition)

# # Filter transformation test
# def test_filter_data(spark, sample_data):
#     expected_data = [
#         {"id": "sam_2", "sample_value": 600},
#         {"id": "sam_3", "sample_value": 750},
#     ]
#     expected_rows = [Row(**x) for x in expected_data]
#     condition = "sample_value > 500"
#     result = filter_data(sample_data, condition)
#     assert result.rdd.collect() == expected_rows

# def test_filter_data_neg(spark, sample_data):
#     expected_data = [
#         {"id": "sam_2", "sample_value": 600},
#         {"id": "sam_3", "sample_value": 750},
#     ]
#     expected_rows = [Row(**x) for x in expected_data]
#     condition = "sample_value_extra > 500"
#     with pytest.raises(Exception):
#         filter_data(sample_data, condition)

# # Filter transformation test
# def test_filter_data(spark, sample_data):
#     expected_data = [
#         {"id": "sam_2", "sample_value": 600},
#         {"id": "sam_3", "sample_value": 750},
#     ]
#     expected_rows = [Row(**x) for x in expected_data]
#     condition = "sample_value > 500"
#     result = filter_data(sample_data, condition)
#     assert result.rdd.collect() == expected_rows

# def test_filter_data_neg(spark, sample_data):
#     expected_data = [
#         {"id": "sam_2", "sample_value": 600},
#         {"id": "sam_3", "sample_value": 750},
#     ]
#     expected_rows = [Row(**x) for x in expected_data]
#     condition = "sample_value_extra > 500"
#     with pytest.raises(Exception):
#         filter_data(sample_data, condition)