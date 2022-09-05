defmodule Operator do
  def parse(operator) do
    case String.trim(operator) do
      "equal" -> "="
      "is equal" -> "="
      "isequal" -> "="
      "is_equal" -> "="
      "is equal to" -> "="
      "is_equal_to" -> "="
      "isequalto" -> "="
      "equals" -> "="
      "is equals" -> "="
      "isequals" -> "="
      "is_equals" -> "="
      "is equals to" -> "="
      "is_equals_to" -> "="
      "isequalsto" -> "="
      "=" -> "="
      "less than" -> "<"
      "less_than" -> "<"
      "lessthan" -> "<"
      "is less than" -> "<"
      "is_less_than" -> "<"
      "islessthan" -> "<"
      "lt" -> "<"
      "<" -> "<"
      "greater than" -> ">"
      "greaterthan" -> ">"
      "greater_than" -> ">"
      "is greater than" -> ">"
      "is_greater_than" -> ">"
      "isgreaterthan" -> ">"
      "gt" -> ">"
      ">" -> ">"
      "less than or equal" -> "<="
      "less_than_or_equal" -> "<="
      "lessthanorequal" -> "<="
      "is less than or equal" -> "<="
      "is_less_than_or_equal" -> "<="
      "islessthanorequal" -> "<="
      "is less than or equals" -> "<="
      "is_less_than_or_equals" -> "<="
      "islessthanorequals" -> "<="
      "is less than or equal to " -> "<="
      "is_less_than_or_equal_to" -> "<="
      "islessthanorequalto" -> "<="
      "is less than or equals to " -> "<="
      "is_less_than_or_equals_to" -> "<="
      "islessthanorequalsto" -> "<="
      "lte" -> "<="
      "<=" -> "<="
      "greater than or equal" -> ">="
      "greater_than_or_equal" -> ">="
      "greaterthanorequal" -> ">="
      "is greater than or equal" -> ">="
      "is_greater_than_or_equal" -> ">="
      "isgreaterthanorequal" -> ">="
      "is greater than or equals" -> ">="
      "is_greater_than_or_equals" -> ">="
      "isgreaterthanorequals" -> ">="
      "is greater than or equal to " -> ">="
      "is_greater_than_or_equal_to" -> ">="
      "isgreaterthanorequalto" -> ">="
      "is greater than or equals to " -> ">="
      "is_greater_than_or_equals_to" -> ">="
      "isgreaterthanorequalsto" -> ">="
      "gte" -> ">="
      ">=" -> ">="
      "and" -> "AND"
      "AND" -> "AND"
      "&" -> "AND"
      "&&" -> "AND"
      "or" -> "OR"
      "OR" -> "OR"
      "|" -> "OR"
      "||" -> "OR"
    end
  end
end