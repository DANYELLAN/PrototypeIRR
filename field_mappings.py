from config import get_env_list


EMPLOYEE_NAME_FIELDS = get_env_list(
    "EMPLOYEE_NAME_FIELDS",
    ["Title", "EmployeeName", "Name", "FullName", "LinkTitle"],
)
EMPLOYEE_ADP_FIELDS = get_env_list(
    "EMPLOYEE_ADP_FIELDS",
    ["ADPNumber", "ADP", "EmployeeNumber", "BadgeNumber", "field_0", "field_1"],
)
EMPLOYEE_STATUS_FIELDS = get_env_list(
    "EMPLOYEE_STATUS_FIELDS",
    ["Status", "field_19"],
)
EMPLOYEE_BRANCH_FIELDS = get_env_list(
    "EMPLOYEE_BRANCH_FIELDS",
    ["field_22", "Branch", "Site", "Location"],
)
EMPLOYEE_DEPARTMENT_FIELDS = get_env_list(
    "EMPLOYEE_DEPARTMENT_FIELDS",
    ["field_20", "Department"],
)
EMPLOYEE_ROLE_FIELDS = get_env_list(
    "EMPLOYEE_ROLE_FIELDS",
    ["PositionTitle", "field_6"],
)
EMPLOYEE_MACHINIST_FIELDS = get_env_list(
    "EMPLOYEE_MACHINIST_FIELDS",
    ["machinist", "Machinist", "IsMachinist", "CNCOperator"],
)

PRODUCTION_NUMBER_FIELDS = get_env_list(
    "PRODUCTION_NUMBER_FIELDS",
    ["ProductionNumber", "ProdOrdID", "WO", "WorkOrder"],
)
PRODUCTION_ORDER_TYPE_FIELDS = get_env_list(
    "PRODUCTION_ORDER_TYPE_FIELDS",
    ["OrderType", "Type"],
)
PRODUCTION_STATUS_FIELDS = get_env_list(
    "PRODUCTION_STATUS_FIELDS",
    ["Status", "OrderStatus"],
)
PRODUCTION_OPERATION_DESCRIPTION_FIELDS = get_env_list(
    "PRODUCTION_OPERATION_DESCRIPTION_FIELDS",
    ["OperationDescription", "Description", "Operation Desc", "Operation_Description"],
)
PRODUCTION_BRANCH_FIELDS = get_env_list(
    "PRODUCTION_BRANCH_FIELDS",
    ["Branch", "Location", "Site"],
)

RECIPE_NAME_FIELDS = get_env_list(
    "RECIPE_NAME_FIELDS",
    ["Title", "RecipeName", "Name"],
)
RECIPE_CONNECTION_TYPE_FIELDS = get_env_list(
    "RECIPE_CONNECTION_TYPE_FIELDS",
    ["ConnectionType", "connectionType"],
)
RECIPE_VERSION_FIELDS = get_env_list(
    "RECIPE_VERSION_FIELDS",
    ["RecipeVersion", "recipeVersion"],
)
RECIPE_JSON_FIELDS = get_env_list(
    "RECIPE_JSON_FIELDS",
    ["RecipeJson", "recipeJson"],
)
RECIPE_MIN_MAX_RULES_FIELDS = get_env_list(
    "RECIPE_MIN_MAX_RULES_FIELDS",
    ["MinMaxRulesJson", "minMaxRulesJson"],
)
RECIPE_APPROVAL_RULES_FIELDS = get_env_list(
    "RECIPE_APPROVAL_RULES_FIELDS",
    ["RequiresApprovalRulesJson", "requiresApprovalRulesJson"],
)
RECIPE_ELEMENT_DESCRIPTION_FIELDS = get_env_list(
    "RECIPE_ELEMENT_DESCRIPTION_FIELDS",
    ["ElementDescription", "Element Description", "Element", "InspectionElement"],
)
RECIPE_DWG_DIM_FIELDS = get_env_list(
    "RECIPE_DWG_DIM_FIELDS",
    ["DWGDIM", "DWG DIM", "DwgDim"],
)
RECIPE_GAUGE_FIELDS = get_env_list(
    "RECIPE_GAUGE_FIELDS",
    ["Gauge", "Gage"],
)
RECIPE_BRANCH_FIELDS = get_env_list(
    "RECIPE_BRANCH_FIELDS",
    ["Branch", "Location", "Site"],
)
