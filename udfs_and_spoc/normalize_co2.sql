
CREATE OR REPLACE FUNCTION ANALYTICS_CO2.NORMALIZE_CO2_UDF(
    CO2_PPM NUMBER(35,4)
)
RETURNS NUMBER(35,4)
AS
$$
    -- Get min/max values for normalization
    WITH min_max AS (
      SELECT 
        MIN(CO2_PPM) AS MIN_CO2, 
        MAX(CO2_PPM) AS MAX_CO2 
      FROM RAW_CO2.CO2_DATA
    )
    SELECT 
      -- Normalize to 0-1 range
      CASE 
        WHEN (SELECT MAX_CO2 FROM min_max) = (SELECT MIN_CO2 FROM min_max) THEN 0.5
        ELSE (CO2_PPM - (SELECT MIN_CO2 FROM min_max)) / 
             ((SELECT MAX_CO2 FROM min_max) - (SELECT MIN_CO2 FROM min_max))
      END
$$;