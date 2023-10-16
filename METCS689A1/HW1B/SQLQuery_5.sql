SELECT 
    state_name,
    SUM(CASE WHEN own_computer = 1 THEN 1 ELSE 0 END) AS number_of_people_owning_computer,
    RANK() OVER (ORDER BY SUM(CASE WHEN own_computer = 1 THEN 1 ELSE 0 END) DESC) AS state_rank
FROM dbo.additional_person_info
GROUP BY state_name
ORDER BY state_rank;
