"""Test schema validation for NWP data transformation."""

import numpy as np
import xarray as xr
import jsonschema
import pytest

from models import PRODUCTS, ForecastModel, AnalysisModel
from shared.utils import load_schema

SCHEMA_DIR = "./schemas"


# ---------------------------------------------------------------------------
# Test data builders
# ---------------------------------------------------------------------------

def build_forecast_test_data(config: ForecastModel) -> xr.Dataset:
    """Build synthetic forecast-shaped data from any ForecastModel config."""
    raw_vars = list(config.variable_mapping.keys())
    hours = config.get_forecast_hours()
    step = np.array(list(hours), dtype="timedelta64[h]")
    n_steps = len(step)
    init_time = np.datetime64("2025-05-06T12:00:00")

    data_vars = {
        var: (["step", "y", "x"], np.random.uniform(0, 10, (n_steps, 2, 2)).astype("float32"))
        for var in raw_vars
    }

    return xr.Dataset(
        data_vars=data_vars,
        coords={
            "step": step,
            "valid_time": ("step", init_time + step),
            "latitude": (["y", "x"], np.array([[30.48, 30.48], [34.99, 34.99]])),
            "longitude": (["y", "x"], np.array([[-99.99, -93.82], [-99.99, -93.82]])),
            "time": init_time,
            "surface": 0.0,
            "gribfile_projection": None,
        },
    )


def build_analysis_test_data(config: AnalysisModel) -> xr.Dataset:
    """Build synthetic analysis-shaped data from any AnalysisModel config."""
    raw_vars = list(config.variable_mapping.keys())
    valid_time = np.datetime64("2026-05-13T18:00:00")

    data_vars = {
        var: (["y", "x"], np.random.uniform(290, 310, (2, 2)).astype("float32"))
        for var in raw_vars
    }

    return xr.Dataset(
        data_vars=data_vars,
        coords={
            "latitude": (["y", "x"], np.array([[30.44, 30.44], [35.06, 35.06]])),
            "longitude": (["y", "x"], np.array([[-99.99, -93.98], [-99.99, -93.98]])),
            "time": np.datetime64("2026-05-13T18:00:00"),
            "step": np.timedelta64(0, "ns"),
            "heightAboveGround": 2.0,
            "valid_time": valid_time,
            "gribfile_projection": None,
        },
    )


def build_test_data(config):
    """Dispatch to the right builder based on model type."""
    if isinstance(config, ForecastModel):
        return build_forecast_test_data(config)
    elif isinstance(config, AnalysisModel):
        return build_analysis_test_data(config)
    raise TypeError(f"Unknown model type: {type(config)}")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("product_name", PRODUCTS.keys())
def test_schema_validation(product_name):
    """Test that each product's transform validates against its schema."""
    config = PRODUCTS[product_name]
    ds = build_test_data(config)
    transformed = config.transform(ds)

    ds_dict = transformed.to_dict(data=False)
    schema = load_schema(f"{SCHEMA_DIR}/{config.schema}")

    try:
        jsonschema.validate(ds_dict, schema)
    except jsonschema.ValidationError as e:
        pytest.fail(f"[{product_name}] Schema validation failed: {e.message}")

    print(f"✓ {product_name} validated. Dims: {dict(transformed.dims)}, Vars: {list(transformed.data_vars.keys())}")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
