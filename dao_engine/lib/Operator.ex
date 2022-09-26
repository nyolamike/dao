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
      "is inside of" -> "IN"
      "is inside" -> "IN"
      "is found in" -> "IN"
      "is contained in" -> "IN"
      "IN" -> "IN"
      "ends with" -> "%LIKE"
      "ends_with" -> "%LIKE"
      "starts with" -> "LIKE%"
      "starts_with" -> "LIKE%"
      "contains" -> "%LIKE%"
      "has" -> "%LIKE%"
      "like" -> "LIKE"
      "matches" -> "LIKE"
    end
  end

  def process_where_clause(_context, _query_config, nil, _str_node_name_key), do: ""

  def process_where_clause(context, query_config, [left, operator, right], str_node_name_key) do
    process_where_clause(context, query_config, {left, operator, right}, str_node_name_key)
  end

  def process_where_clause(_context, _query_config, where_sql, _str_node_name_key)
      when is_binary(where_sql),
      do: where_sql

  def process_where_clause(context, query_config, {left, operator, right}, str_node_name_key) do
    operator_sql = Operator.parse(operator)
    preped_operator_sql = Operator.get_possible_operator(context, operator_sql)

    cond do
      is_tuple(left) == false && is_tuple(right) == false ->
        left_side = Table.sql_table_column_name(context, str_node_name_key, left)
        # if Map.get(context, "track_id") == "4664" do
        #   IO.inspect(query_config)
        #   left_side = Table.sql_table_column_name(context, str_node_name_key, left_side)
        #   IO.inspect(left_side)
        # end
        possible_value = get_possible_value(context, operator_sql, right)

        case possible_value do
          {cntxt, pv} -> {cntxt, "#{left_side} #{preped_operator_sql} #{pv}"}
          pv -> {context, "#{left_side} #{preped_operator_sql} #{pv}"}
        end

      is_tuple(left) == false && is_tuple(right) == true ->
        right_side = process_where_clause(context, query_config, right, str_node_name_key)

        case right_side do
          {cntxt, rs} -> {cntxt, "(#{left}) #{preped_operator_sql} #{rs}"}
          rs -> {context, "(#{left}) #{preped_operator_sql} #{rs}"}
        end

      is_tuple(left) == true && is_tuple(right) == false ->
        left_side = process_where_clause(context, query_config, right, str_node_name_key)
        possible_value = Column.sql_value_format(right)

        case left_side do
          {cntxt, ls} -> {cntxt, "#{ls} #{preped_operator_sql} (#{possible_value})"}
          ls -> {context, "#{ls} #{preped_operator_sql} (#{possible_value})"}
        end

      is_tuple(left) == true && is_tuple(right) == true ->
        right_side = process_where_clause(context, query_config, right, str_node_name_key)

        {context, right_side} =
          case right_side do
            {cntxt, rs} -> {cntxt, rs}
            rs -> {context, rs}
          end

        left_side = process_where_clause(context, query_config, left, str_node_name_key)

        {context, left_side} =
          case left_side do
            {cntxt, ls} -> {cntxt, ls}
            ls -> {context, ls}
          end

        {context, "(#{left_side}) #{preped_operator_sql} (#{right_side})"}
    end
  end

  def get_possible_value(context, operator_sql, right) do
    case operator_sql do
      "IN" ->
        # we need to wrap the items into brackets ()
        cond do
          is_list(right) ->
            if Keyword.keyword?(right) do
              # is probably a nested value scenario
              value_format(context, right)
            else
              # its a list of possible values
              temp_sql =
                Enum.reduce(right, "", fn item, acc ->
                  possible_value = Column.sql_value_format(item)
                  comma = if acc == "", do: "", else: ", "
                  "#{acc}#{comma}#{possible_value}"
                end)

              "(#{temp_sql})"
            end

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

      "%LIKE" ->
        "'%#{right}'"

      "LIKE%" ->
        "'#{right}%'"

      "%LIKE%" ->
        "'%#{right}%'"

      _ ->
        value_format(context, right)
    end
  end

  def get_possible_operator(context, operator_sql) do
    likes = ["%LIKE", "LIKE%", "LIKE", "%LIKE%"]
    if operator_sql in likes, do: "LIKE", else: operator_sql
  end

  def value_format(context, value) do
    if is_list(value) && Keyword.keyword?(value) do
      # this is probaly a nested query scenario
      nested_query_res = DaoEngine.gen_sql_for_get_fixture(context, [], value)
      context_res = nested_query_res["context"]
      {_table_key, %{"sql" => nested_sql}} = nested_query_res["fixture_list"] |> hd()
      {context_res, "(#{nested_sql})"}
    else
      Column.sql_value_format(value)
    end
  end
end
