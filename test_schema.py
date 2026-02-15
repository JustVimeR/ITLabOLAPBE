from models import DimRegion
from schemas import DimBase

def test_dim_region_schema():
    print("Testing DimRegion model compatibility with Pydantic schema...")
    
    region = DimRegion(id=1, region_name="North", city="Winterfell")
    
    print(f"Region Name Property: {region.name}")
    
    try:
        pydantic_obj = DimBase.model_validate(region)
        print(f"Pydantic Validation Success: {pydantic_obj}")
    except Exception as e:
        print(f"Pydantic Validation Failed: {e}")

if __name__ == "__main__":
    test_dim_region_schema()
