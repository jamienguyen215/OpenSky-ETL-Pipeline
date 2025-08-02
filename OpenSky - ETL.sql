SELECT COUNT (*) AS Total_records,
       COUNT (DISTINCT nearest_airport) AS Unique_airport,
	   COUNT (*) FILTER(WHERE nearest_airport IS NOT NULL) AS Grounded_planes
FROM "OpenSkyApi";


SELECT nearest_airport,COUNT (*) AS flight_count FROM "OpenSkyApi"
WHERE nearest_airport IS NOT NULL
GROUP BY nearest_airport
ORDER BY flight_count DESC;


SELECT datetime AS Date,
       COUNT (*) AS flight_count 
	   FROM "OpenSkyApi"
WHERE nearest_airport IS NOT NULL
GROUP BY nearest_airport, Date
ORDER BY flight_count DESC;

SELECT nearest_airport AS Airport, datetime AS Date,
       COUNT (*) AS flight_count 
	   FROM "OpenSkyApi"
WHERE nearest_airport IS NOT NULL
GROUP BY nearest_airport, Date
ORDER BY flight_count DESC;


