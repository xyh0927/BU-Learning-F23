SELECT 
    dbo.additional_person_info.state_name,
    dbo.additional_person_info.edu_code_description AS education_level,
    COUNT(*) AS number_of_people_responding,
    SUM(CASE WHEN dbo.additional_person_info.own_computer = 1 THEN 1 ELSE 0 END) AS number_of_people_owning_computer,
    AVG(dbo.additional_person_info.income) AS average_income
FROM dbo.additional_person_info
GROUP BY CUBE (dbo.additional_person_info.state_name, dbo.additional_person_info.edu_code_description)
ORDER BY dbo.additional_person_info.state_name ASC, dbo.additional_person_info.edu_code_description ASC;
