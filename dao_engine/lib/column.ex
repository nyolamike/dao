defmodule Column do
  def define("pk") do
    %{
      "type" => "integer",
      "size" => 30,
      "default" => nil,
      "auto_increment" => true,
      "is_primary_key" => true,
      "required" => false,
      "sql" => "INT(30) AUTO_INCREMENT NOT NULL PRIMARY KEY"
    }
  end

  def define("integer"), do: define(%{"type" => "integer"})

  def define("int"), do: define(%{"type" => "integer"})

  def define(%{"type" => "int"} = config), do: define(%{config | "type" => "integer"})

  def define(%{"type" => "integer"} = config) do
    # default is 30
    size = if Map.has_key?(config, "size"), do: config["size"], else: 30
    # default is false
    is_primary_key =
      if Map.has_key?(config, "is_primary_key"), do: config["is_primary_key"], else: false

    # default is 0
    default = if Map.has_key?(config, "default"), do: config["default"], else: ""
    default_sql = if default != "", do: " DEFAULT '#{default}'", else: ""
    # default is o, meaning it allows nil/null values
    required = if Map.has_key?(config, "required"), do: config["required"], else: false
    required_sql = if required == true, do: " NOT NULL", else: ""

    # unique default value is false which produces an empty sql
    unique = if Map.has_key?(config, "unique"), do: config["unique"], else: false
    unique_sql = if unique == true, do: " UNIQUE", else: ""

    # auto increment default value is false which produces an empty sql
    auto_increment =
      if Map.has_key?(config, "auto_increment"), do: config["auto_increment"], else: false

    auto_increment_sql = if auto_increment == true, do: " AUTO_INCREMENT", else: ""

    # nyd: validation of these column properties from user input

    %{
      "type" => "integer",
      "size" => size,
      "default" => default,
      "auto_increment" => auto_increment,
      "is_primary_key" => is_primary_key,
      "required" => required,
      "unique" => unique,
      "sql" =>
        String.trim("INT(#{size})#{required_sql}#{default_sql}#{unique_sql}#{auto_increment_sql}")
    }
  end

  def define("boolean"), do: define(%{"type" => "boolean"})

  def define(%{"type" => "boolean"} = config) do
    # default is 1
    size = if Map.has_key?(config, "size"), do: config["size"], else: 1
    # default is false
    is_primary_key =
      if Map.has_key?(config, "is_primary_key"), do: config["is_primary_key"], else: false

    # default is 0/false
    default = if Map.has_key?(config, "default"), do: config["default"], else: nil
    default_sql = if default != nil, do: " DEFAULT #{default}", else: " DEFAULT 0"
    # default is true, meaning it does not allow nil/null values
    required = if Map.has_key?(config, "required"), do: config["required"], else: true
    required_sql = if required == true, do: " NOT NULL", else: ""

    # unique default value is false which produces an empty sql
    unique = if Map.has_key?(config, "unique"), do: config["unique"], else: false
    unique_sql = if unique == true, do: " UNIQUE", else: ""

    # auto increment default value is false which produces an empty sql
    auto_increment =
      if Map.has_key?(config, "auto_increment"), do: config["auto_increment"], else: false

    auto_increment_sql = if auto_increment == true, do: " AUTO_INCREMENT", else: ""

    %{
      "type" => "boolean",
      "size" => size,
      "default" => default,
      "auto_increment" => auto_increment,
      "is_primary_key" => is_primary_key,
      "required" => required,
      "unique" => unique,
      "sql" =>
        String.trim(
          "TINYINT(#{size})#{required_sql}#{default_sql}#{unique_sql}#{auto_increment_sql}"
        )
    }
  end

  def define("string"), do: define(%{"type" => "string"})

  def define(%{"type" => "string"} = config) do
    # default is 30
    size = if Map.has_key?(config, "size"), do: config["size"], else: 30
    # default is false
    is_primary_key =
      if Map.has_key?(config, "is_primary_key"), do: config["is_primary_key"], else: false

    # default is ""
    default = if Map.has_key?(config, "default"), do: config["default"], else: ""
    default_sql = if default != "", do: " DEFAULT '#{default}'", else: ""
    # default is false, meaning it allows nil/null values
    required = if Map.has_key?(config, "required"), do: config["required"], else: false
    required_sql = if required == true, do: " NOT NULL", else: ""

    # unique default value is false which produces an empty sql
    unique = if Map.has_key?(config, "unique"), do: config["unique"], else: false
    unique_sql = if unique == true, do: " UNIQUE", else: ""

    # auto increment default value is false which produces an empty sql
    auto_increment =
      if Map.has_key?(config, "auto_increment"), do: config["auto_increment"], else: false

    auto_increment_sql = if auto_increment == true, do: " AUTO_INCREMENT", else: ""

    %{
      "type" => "string",
      "size" => size,
      "default" => default,
      "auto_increment" => auto_increment,
      "is_primary_key" => is_primary_key,
      "required" => required,
      "unique" => unique,
      "sql" =>
        String.trim(
          "VARCHAR(#{size})#{required_sql}#{default_sql}#{unique_sql}#{auto_increment_sql}"
        )
    }
  end

  def define("timestamp"), do: define(%{"type" => "timestamp"})

  def define(%{"type" => "timestamp"} = config) do
    # default is 40
    size = if Map.has_key?(config, "size"), do: config["size"], else: 40
    # default is false
    is_primary_key =
      if Map.has_key?(config, "is_primary_key"), do: config["is_primary_key"], else: false

    # default is 0
    default = if Map.has_key?(config, "default"), do: config["default"], else: 0

    default_sql =
      if default != nil, do: " DEFAULT #{default}", else: " DEFAULT (UNIX_TIMESTAMP())"

    # default is false, meaning it allows nil/null values
    required = if Map.has_key?(config, "required"), do: config["required"], else: false
    required_sql = if required == true, do: " NOT NULL", else: ""

    # unique default value is false which produces an empty sql
    unique = if Map.has_key?(config, "unique"), do: config["unique"], else: false
    unique_sql = if unique == true, do: " UNIQUE", else: ""

    # auto increment default value is false which produces an empty sql
    auto_increment =
      if Map.has_key?(config, "auto_increment"), do: config["auto_increment"], else: false

    auto_increment_sql = if auto_increment == true, do: " AUTO_INCREMENT", else: ""

    %{
      "type" => "timestamp",
      "size" => size,
      "default" => default,
      "auto_increment" => auto_increment,
      "is_primary_key" => is_primary_key,
      "required" => required,
      "unique" => unique,
      "sql" =>
        String.trim(
          "BIGINT(#{size})#{required_sql}#{default_sql}#{unique_sql}#{auto_increment_sql}"
        )
    }
  end

  def define("datetime"), do: define(%{"type" => "datetime"})

  def define(%{"type" => "datetime"} = config) do
    # default is 60
    size = if Map.has_key?(config, "size"), do: config["size"], else: 60
    # default is false
    is_primary_key =
      if Map.has_key?(config, "is_primary_key"), do: config["is_primary_key"], else: false

    # default is nil
    default = if Map.has_key?(config, "default"), do: config["default"], else: nil

    default_sql = if default != nil, do: " DEFAULT #{default}", else: " DEFAULT CURRENT_TIMESTAMP"

    # default is false, meaning it allows nil/null values
    required = if Map.has_key?(config, "required"), do: config["required"], else: false
    required_sql = if required == true, do: " NOT NULL", else: ""

    # unique default value is false which produces an empty sql
    unique = if Map.has_key?(config, "unique"), do: config["unique"], else: false
    unique_sql = if unique == true, do: " UNIQUE", else: ""

    # auto increment default value is false which produces an empty sql
    auto_increment =
      if Map.has_key?(config, "auto_increment"), do: config["auto_increment"], else: false

    auto_increment_sql = if auto_increment == true, do: " AUTO_INCREMENT", else: ""

    %{
      "type" => "datetime",
      "size" => size,
      "default" => default,
      "auto_increment" => auto_increment,
      "is_primary_key" => is_primary_key,
      "required" => "",
      "unique" => unique,
      "sql" =>
        String.trim("DATETIME#{required_sql}#{default_sql}#{unique_sql}#{auto_increment_sql}")
    }
  end

  def define("date"), do: define(%{"type" => "date"})

  def define(%{"type" => "date"} = config) do
    # default is false
    is_primary_key =
      if Map.has_key?(config, "is_primary_key"), do: config["is_primary_key"], else: false

    # default is nil
    default = if Map.has_key?(config, "default"), do: config["default"], else: nil

    default_sql = if default != nil, do: " DEFAULT #{default}", else: " DEFAULT CURRENT_TIMESTAMP"

    # nyd: define default for date as the current day
    # default is false, meaning it allows nil/null values
    required = if Map.has_key?(config, "required"), do: config["required"], else: false
    required_sql = if required == true, do: " NOT NULL", else: ""

    # unique default value is false which produces an empty sql
    unique = if Map.has_key?(config, "unique"), do: config["unique"], else: false
    unique_sql = if unique == true, do: " UNIQUE", else: ""

    # auto increment default value is false which produces an empty sql
    auto_increment =
      if Map.has_key?(config, "auto_increment"), do: config["auto_increment"], else: false

    auto_increment_sql = if auto_increment == true, do: " AUTO_INCREMENT", else: ""

    %{
      "type" => "date",
      "default" => default,
      "auto_increment" => auto_increment,
      "is_primary_key" => is_primary_key,
      "required" => "",
      "unique" => unique,
      "sql" =>
        String.trim("DATETIME#{required_sql}#{default_sql}#{unique_sql}#{auto_increment_sql}")
    }
  end

  def define("float"), do: define(%{"type" => "decimal"})

  def define(%{"type" => "float"} = config) do
    define(%{config | "type" => "decimal"})
  end

  def define("decimal"), do: define(%{"type" => "decimal"})

  def define(%{"type" => "decimal"} = config) do
    # default is 12
    size = if Map.has_key?(config, "size"), do: config["size"], else: 12
    # default is false
    is_primary_key =
      if Map.has_key?(config, "is_primary_key"), do: config["is_primary_key"], else: false

    # default is 0.0
    default = if Map.has_key?(config, "default"), do: config["default"], else: nil

    # default_sql = if default != nil, do: "DEFAULT #{default}", else: "DEFAULT 0.0"
    default_sql = if default != nil, do: " DEFAULT #{default}", else: ""

    # default is false, meaning it allows nil/null values
    required = if Map.has_key?(config, "required"), do: config["required"], else: false
    required_sql = if required == true, do: " NOT NULL", else: ""

    # default is 0.0
    decimal_places =
      if Map.has_key?(config, "decimal_places"), do: config["decimal_places"], else: 6

    # unique default value is false which produces an empty sql
    unique = if Map.has_key?(config, "unique"), do: config["unique"], else: false
    unique_sql = if unique == true, do: " UNIQUE", else: ""

    # auto increment default value is false which produces an empty sql
    auto_increment =
      if Map.has_key?(config, "auto_increment"), do: config["auto_increment"], else: false

    auto_increment_sql = if auto_increment == true, do: " AUTO_INCREMENT", else: ""

    %{
      "type" => "decimal",
      "size" => size,
      "decimal_places" => decimal_places,
      "default" => default,
      "auto_increment" => auto_increment,
      "is_primary_key" => is_primary_key,
      "required" => "",
      "unique" => unique,
      "sql" =>
        String.trim(
          "DECIMAL(#{size},#{decimal_places})#{required_sql}#{default_sql}#{unique_sql}#{auto_increment_sql}"
        )
    }
  end

  def define("blob"), do: define(%{"type" => "blob"})

  def define(%{"type" => "blob"} = config) do
    # nyd: define blob data type implementation
    %{}
  end

  def define("text"), do: define(%{"type" => "text"})

  def define(%{"type" => "text"} = config) do
    # default is false
    is_primary_key =
      if Map.has_key?(config, "is_primary_key"), do: config["is_primary_key"], else: false

    # default is null
    default = if Map.has_key?(config, "default"), do: config["default"], else: nil

    default_sql = if default != nil, do: " DEFAULT #{default}", else: " DEFAULT NULL"

    # default is false, meaning it allows nil/null values
    required = if Map.has_key?(config, "required"), do: config["required"], else: false
    required_sql = if required == true, do: " NOT NULL", else: ""

    # unique default value is false which produces an empty sql
    unique = if Map.has_key?(config, "unique"), do: config["unique"], else: false
    unique_sql = if unique == true, do: " UNIQUE", else: ""

    # auto increment default value is false which produces an empty sql
    auto_increment =
      if Map.has_key?(config, "auto_increment"), do: config["auto_increment"], else: false

    auto_increment_sql = if auto_increment == true, do: " AUTO_INCREMENT", else: ""

    %{
      "type" => "text",
      "default" => default,
      "auto_increment" => auto_increment,
      "is_primary_key" => is_primary_key,
      "required" => "",
      "unique" => unique,
      "sql" => String.trim("TEXT#{required_sql}#{default_sql}#{unique_sql}#{auto_increment_sql}")
    }
  end

  def define(probably_a_value) do
    probably_a_value |> value_to_col_type() |> define()
  end

  def define_columnx(%{} = context, "use_primary_keys", list) when is_list(list) do
    sql = if length(list) > 0, do: "PRIMARY KEY(#{Enum.join(list, ", ")})", else: ""

    %{
      "sql" => sql,
      "config" => %{}
    }
  end

  def define_column(context, column_name, type) do
    # the context is important incase of different database types
    # using different syntax or incase of a config that may specify different things
    config = define(type)

    column_name = if is_atom(column_name), do: Atom.to_string(column_name), else: column_name

    %{
      "sql" => "#{column_name} #{config["sql"]}",
      "config" => config
    }
  end

  def gen_sql_columns(context, plural_table_name, query_config) do
    # checks to see if the columns in the query_config are all inside of the schema
    if Map.has_key?(query_config, "columns") == true do
      acc = %{
        "sql" => "",
        "table_schema" => context["schema"][plural_table_name],
        "errors" => %{}
      }

      Enum.reduce(query_config["columns"], acc, fn {column_name_key, column_config}, acc ->
        sql_acc = String.trim(acc["sql"])
        table_schema = acc["table_schema"]

        if Map.has_key?(table_schema, column_name_key) == false do
          col_def = define_column(context, column_name_key, column_config)
          # update the schema
          schema = Map.put(table_schema, column_name_key, col_def["config"])
          comma = if sql_acc == "", do: "", else: ", "
          sql = sql_acc <> comma <> "ADD " <> String.trim(col_def["sql"])
          %{"sql" => sql, "table_schema" => schema, "errors" => acc["errors"]}
        else
          # add an error in case we are in the add situation
          # nyd: detect if we are in the add situation
          msg = "Column " <> column_name_key <> ", already exists"
          errors = Map.put(acc["errors"], column_name_key, msg)
          %{"sql" => sql_acc, "table_schema" => table_schema, "errors" => errors}
        end
      end)
    else
      %{
        "sql" => "",
        "table_schema" => %{},
        "errors" => %{}
      }
    end
  end

  def value_to_col_type(value) when is_integer(value), do: "integer"
  def value_to_col_type(value) when is_float(value), do: "decimal"
  def value_to_col_type(value) when is_boolean(value), do: "boolean"

  def value_to_col_type(value) when is_binary(value) do
    len = String.length(value)

    cond do
      len <= 100 -> "string"
      true -> "text"
    end
  end

  def value_to_col_type(value) when value, do: "boolean"
  # nyd: value_to_col_type for dates specifiesd as either date objects or strings
  # nyd: how to detect datetime values and timestamps
  # nyd: how to detect blobs

  def list_to_query_config(context, plural_table_name, query_config) do
    # for example in insert queries
    item_to_scan = hd(query_config)
    item_to_scan = if is_list(item_to_scan), do: hd(item_to_scan), else: query_config
    # Utils.log("query_config", item_to_scan)
    # Utils.log("schema", context["schema"][plural_table_name])
    # we need to match the entries of the array with table columns
    acc = %{
      next_col_no: 1,
      cols: %{}
    }

    schema_cols =
      Enum.reduce(context["schema"][plural_table_name], acc, fn {key, value}, acc ->
        if String.starts_with?(key, "dao@") do
          acc
        else
          cols = Map.put(acc.cols, acc.next_col_no, {key, value})
          %{acc | next_col_no: acc.next_col_no + 1, cols: cols}
        end
      end)

    # Utils.log("schema_cols", schema_cols)

    matching_acc = %{
      next_col_no: 1,
      cols: %{},
      schema_cols: schema_cols.cols
    }

    cols_from_array =
      Enum.reduce(item_to_scan, matching_acc, fn value, acc ->
        cols =
          if Map.has_key?(acc.schema_cols, acc.next_col_no) do
            {col_name, col_schema_def} = acc.schema_cols[acc.next_col_no]
            Map.put(acc.cols, col_name, col_schema_def)
          else
            # we automatically attemp to create a new column
            col_name = "col_#{acc.next_col_no}"
            col_schema_def = Column.value_to_col_type(value)
            Map.put(acc.cols, col_name, col_schema_def)
          end

        %{acc | next_col_no: acc.next_col_no + 1, cols: cols}
      end)

    # Utils.log("cols_from_array", cols_from_array)
    cols_from_array.cols
  end

  def sql_value_format(value) when is_binary(value), do: "'#{value}'"
  def sql_value_format(value) when is_map(value), do: "hi"
  def sql_value_format(value), do: "#{value}"
end
