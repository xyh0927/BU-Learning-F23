SELECT dbo.additional_person_info.state_name, COUNT(*) AS number_of_people_owning_computer
FROM dbo.additional_person_info
WHERE dbo.additional_person_info.own_computer = 1
GROUP BY dbo.additional_person_info.state_name
ORDER BY number_of_people_owning_computer DESC;

