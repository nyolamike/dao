defmodule OrderBy do
  def preprocess_config(context, preprocess_config_table_def, config_table_def) do
    preprocess_config_table_def =
      cond do
        Map.has_key?(config_table_def, "dao@order_by") ->
          Map.put(preprocess_config_table_def, "order_by", config_table_def["dao@order_by"])

        Map.has_key?(config_table_def, "dao@order_by_ascending") ->
          Map.put(
            preprocess_config_table_def,
            "order_by_ascending",
            config_table_def["dao@order_by_ascending"]
          )

        Map.has_key?(config_table_def, "dao@order_asc") ->
          Map.put(
            preprocess_config_table_def,
            "order_by_ascending",
            config_table_def["dao@order_asc"]
          )

        Map.has_key?(config_table_def, "dao@ascending") ->
          Map.put(
            preprocess_config_table_def,
            "order_by_ascending",
            config_table_def["dao@ascending"]
          )

        Map.has_key?(config_table_def, "dao@asc") ->
          Map.put(preprocess_config_table_def, "order_by_ascending", config_table_def["dao@asc"])

        Map.has_key?(config_table_def, "dao@ascend") ->
          Map.put(
            preprocess_config_table_def,
            "order_by_ascending",
            config_table_def["dao@ascend"]
          )

        Map.has_key?(config_table_def, "dao@order_asc") ->
          Map.put(
            preprocess_config_table_def,
            "order_by_ascending",
            config_table_def["dao@order_asc"]
          )

        Map.has_key?(config_table_def, "dao@order_ascending") ->
          Map.put(
            preprocess_config_table_def,
            "order_by_ascending",
            config_table_def["dao@order_ascending"]
          )

        Map.has_key?(config_table_def, "dao@order_by_descending") ->
          Map.put(
            preprocess_config_table_def,
            "order_by_descending",
            config_table_def["dao@order_by_descending"]
          )

        Map.has_key?(config_table_def, "dao@order_by_desc") ->
          Map.put(
            preprocess_config_table_def,
            "order_by_descending",
            config_table_def["dao@order_by_desc"]
          )

        Map.has_key?(config_table_def, "dao@order_desc") ->
          Map.put(
            preprocess_config_table_def,
            "order_by_descending",
            config_table_def["dao@order_desc"]
          )

        Map.has_key?(config_table_def, "dao@descending") ->
          Map.put(
            preprocess_config_table_def,
            "order_by_descending",
            config_table_def["dao@descending"]
          )

        Map.has_key?(config_table_def, "dao@asc") ->
          Map.put(preprocess_config_table_def, "order_by_descending", config_table_def["dao@asc"])

        Map.has_key?(config_table_def, "dao@descend") ->
          Map.put(
            preprocess_config_table_def,
            "order_by_descending",
            config_table_def["dao@descend"]
          )

        Map.has_key?(config_table_def, "dao@order_asc") ->
          Map.put(
            preprocess_config_table_def,
            "order_by_descending",
            config_table_def["dao@order_asc"]
          )

        Map.has_key?(config_table_def, "dao@order_descending") ->
          Map.put(
            preprocess_config_table_def,
            "order_by_descending",
            config_table_def["dao@order_descending"]
          )

        true ->
          preprocess_config_table_def
      end

    {context, preprocess_config_table_def}
  end

  def gen_sql(context, query_config) do
    order_by_sql =
      if is_map(query_config) do
        processed_query_config = Table.preprocess_query_config(context, query_config)

        cond do
          Map.has_key?(processed_query_config, "order_by") &&
              is_binary(processed_query_config["order_by"]) ->
            # one has specified using string so we trim and split using spaces
            orderby_list =
              processed_query_config["order_by"]
              |> String.trim()
              |> String.split(trim: true)

            Enum.reduce(orderby_list, "", fn column, sql_acc ->
              comma = if sql_acc == "", do: "", else: ", "
              "#{sql_acc}#{comma}#{column}"
            end)
            |> String.trim()

          Map.has_key?(processed_query_config, "order_by") &&
              is_list(processed_query_config["order_by"]) ->
            Enum.reduce(processed_query_config["order_by"], "", fn column, sql_acc ->
              comma = if sql_acc == "", do: "", else: ", "
              "#{sql_acc}#{comma}#{column}"
            end)
            |> String.trim()

          Map.has_key?(processed_query_config, "order_by_ascending") &&
              is_binary(processed_query_config["order_by_ascending"]) ->
            # one has specified using string so we trim and split using spaces
            orderby_list =
              processed_query_config["order_by_ascending"]
              |> String.trim()
              |> String.split(trim: true)

            temp_sql =
              Enum.reduce(orderby_list, "", fn column, sql_acc ->
                comma = if sql_acc == "", do: "", else: ", "
                "#{sql_acc}#{comma}#{column}"
              end)

            "#{String.trim(temp_sql)} ASC"

          Map.has_key?(processed_query_config, "order_by_ascending") &&
              is_list(processed_query_config["order_by_ascending"]) ->
            temp_sql =
              Enum.reduce(processed_query_config["order_by_ascending"], "", fn column, sql_acc ->
                comma = if sql_acc == "", do: "", else: ", "
                "#{sql_acc}#{comma}#{column}"
              end)

            "#{String.trim(temp_sql)} ASC"

          Map.has_key?(processed_query_config, "order_by_descending") &&
              is_binary(processed_query_config["order_by_descending"]) ->
            # one has specified using string so we trim and split using spaces
            orderby_list =
              processed_query_config["order_by_descending"]
              |> String.trim()
              |> String.split(trim: true)

            temp_sql =
              Enum.reduce(orderby_list, "", fn column, sql_acc ->
                comma = if sql_acc == "", do: "", else: ", "
                "#{sql_acc}#{comma}#{column}"
              end)

            "#{String.trim(temp_sql)} DEC"

          Map.has_key?(processed_query_config, "order_by_descending") &&
              is_list(processed_query_config["order_by_descending"]) ->
            temp_sql =
              Enum.reduce(processed_query_config["order_by_descending"], "", fn column, sql_acc ->
                comma = if sql_acc == "", do: "", else: ", "
                "#{sql_acc}#{comma}#{column}"
              end)

            "#{String.trim(temp_sql)} DESC"

          true ->
            ""
        end
      else
        ""
      end

    if order_by_sql == "", do: "", else: " ORDER BY #{order_by_sql}"
  end
end
