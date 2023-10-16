CREATE VIEW additional_person_info AS
SELECT dbo.person_economic_info.*, 
       dbo.states.us_state_terr as state_name, 
       dbo.employment_categories.category_description as cat_description, 
       dbo.education_codes.education_level_achieved as edu_code_description
FROM person_economic_info
LEFT JOIN dbo.states ON dbo.person_economic_info.address_state = dbo.states.numeric_id
LEFT JOIN dbo.employment_categories ON dbo.person_economic_info.employment_category = dbo.employment_categories.employment_category
LEFT JOIN dbo.education_codes ON dbo.person_economic_info.education = dbo.education_codes.code;
