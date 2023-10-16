SELECT dbo.additional_person_info.state_name
FROM dbo.additional_person_info
GROUP BY dbo.additional_person_info.state_name
HAVING SUM(CASE WHEN dbo.additional_person_info.own_computer = 1 THEN 1 ELSE 0 END) = 0;
