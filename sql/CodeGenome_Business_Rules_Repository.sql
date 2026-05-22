SELECT
    file_name,
    file_type,
    rule_category,
    business_rule,
    confidence,
    modernization_recommendation
FROM workspace.default.codegenome_business_rules
ORDER BY rule_category, file_name;