SELECT 
    state_name,
    FORMAT(AVG(income), 'C', 'en-us') AS average_income,
    FORMAT(LEAD(AVG(income)) OVER (ORDER BY state_name), 'C', 'en-us') AS next_state_average_income
FROM dbo.additional_person_info
GROUP BY state_name
ORDER BY state_name;