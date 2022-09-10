defmodule Operator do
  def parse(operator) do
    case String.trim(operator) do
      "is" -> "="
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
      "e" -> "="
      "=" -> "="
      "==" -> "="
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
      "not equal" -> "<>"
      "not_equal" -> "<>"
      "notequal" -> "<>"
      "is not equal" -> "<>"
      "is_not_equal" -> "<>"
      "isnotequal" -> "<>"
      "is not equal to" -> "<>"
      "is_not_equal_to" -> "<>"
      "isnotequalto" -> "<>"
      "is not equals to" -> "<>"
      "is_not_equals_to" -> "<>"
      "isnotequalsto" -> "<>"
      "does not equal" -> "<>"
      "does_not_equal" -> "<>"
      "doesnotequal" -> "<>"
      "does not equal to" -> "<>"
      "does_not_equal_to" -> "<>"
      "doesnotequalto" -> "<>"
      "does not equals to" -> "<>"
      "does_not_equals_to" -> "<>"
      "doesnotequalsto" -> "<>"
      "ne" -> "<>"
      "net" -> "<>"
      "ine" -> "<>"
      "inet" -> "<>"
      "dne" -> "<>"
      "dnet" -> "<>"
      "!=" -> "<>"
      "!==" -> "<>"
      "<>" -> "<>"
      "in" -> "IN"
      "is in" -> "IN"
      "IN" -> "IN"
    end
  end

  def process_where_clause(_context, _query_config, nil), do: ""

  def process_where_clause(context, query_config, [left, operator, right]) do
    process_where_clause(context, query_config, {left, operator, right})
  end

  def process_where_clause(_context, _query_config, where_sql) when is_binary(where_sql),
    do: where_sql

  def process_where_clause(context, query_config, {left, operator, right}) do
    operator_sql = Operator.parse(operator)

    cond do
      is_tuple(left) == false && is_tuple(right) == false ->
        left_side = left
        possible_value = get_possible_value(context, operator_sql, right)
        "#{left_side} #{operator_sql} #{possible_value}"

      is_tuple(left) == false && is_tuple(right) == true ->
        right_side = process_where_clause(context, query_config, right)
        "(#{left}) #{operator_sql} #{right_side}"

      is_tuple(left) == true && is_tuple(right) == false ->
        left_side = process_where_clause(context, query_config, right)
        possible_value = Column.sql_value_format(right)
        "#{left_side} #{operator_sql} (#{possible_value})"

      is_tuple(left) == true && is_tuple(right) == true ->
        left_side = process_where_clause(context, query_config, left)
        right_side = process_where_clause(context, query_config, right)
        "(#{left_side}) #{operator_sql} (#{right_side})"
    end
  end

  def get_possible_value(context, operator_sql, right) do
    case operator_sql do
      "IN" ->
        # we need to wrap the items into brackets ()
        cond do
          is_list(right) ->
            # its a list of possible values
            temp_sql =
              Enum.reduce(right, "", fn item, acc ->
                possible_value = Column.sql_value_format(item)
                comma = if acc == "", do: "", else: ", "
                "#{acc}#{comma}#{possible_value}"
              end)

            "(#{temp_sql})"

          is_binary(right) ->
            # a string
            # in future this has to be scanned for security reasons and proper formating
            right = String.trim(right)

            if String.starts_with?(right, "(") do
              right
            else
              # we assume no wrapping
              "(#{right})"
            end
        end

      _ ->
        Column.sql_value_format(right)
    end
  end
end
