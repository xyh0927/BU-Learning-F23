SELECT 
    state_name,
    COUNT(*) AS number_of_people_reported,
    SUM(CASE WHEN internet = 1 THEN 1 ELSE 0 END) AS number_of_people_use_internet,
    SUM(CASE WHEN own_computer = 1 THEN 1 ELSE 0 END) AS number_of_people_own_computer,
    FORMAT(MAX(income), 'C', 'en-us') AS highest_income,
    FORMAT(AVG(income), 'C', 'en-us') AS average_income,
    ISNULL(CAST(SUM(CASE WHEN own_computer = 1 AND internet = 1 THEN 1 ELSE 0 END) AS FLOAT) 
    / NULLIF(SUM(CASE WHEN own_computer = 1 THEN 1 ELSE 0 END), 0) * 100, 0) AS percentage_of_pc_owners_with_internet
FROM dbo.additional_person_info
WHERE EXISTS (
    SELECT 1 
    FROM additional_person_info AS sub
    WHERE sub.state_name = additional_person_info.state_name
    AND sub.own_computer = 1
)
GROUP BY state_name;
