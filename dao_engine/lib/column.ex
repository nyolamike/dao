defmodule Column do
  @spec define(boolean | binary | number | map) :: any
  def define(%{"fk" => table_name} = config) do
    plural_table_name = Inflex.pluralize(table_name)

    # a fk can be null depending on the delete definition
    on_delete = if Map.has_key?(config, "on_delete"), do: config["on_delete"], else: "cascade"
    required_sql = if on_delete == "cascade", do: " NOT NULL", else: ""

    %{
      "type" => "integer",
      "size" => 30,
      "default" => nil,
      "auto_increment" => false,
      "is_primary_key" => false,
      "required" => false,
      "is_foreign_key" => true,
      "fk" => plural_table_name,
      "on" => config["on"],
      "on_delete" => config["on_delete"],
      "sql" => "INT(30)#{required_sql}"
    }
  end

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

  def define("bool"), do: define(%{"type" => "boolean"})

  def define(%{"type" => "bool"} = config), do: define(%{config | "type" => "boolean"})

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

  def define("str"), do: define(%{"type" => "string"})

  def define(%{"type" => "str"} = config), do: define(%{config | "type" => "string"})

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

  def define("date"), do: define(%{"type" => "date"})

  def define(%{"type" => "date"} = config) do
    # default is false
    is_primary_key =
      if Map.has_key?(config, "is_primary_key"), do: config["is_primary_key"], else: false

    # default is nil
    default = if Map.has_key?(config, "default"), do: config["default"], else: nil

    default_sql = if default != nil, do: " DEFAULT #{default}", else: ""

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
      "sql" => String.trim("DATE#{required_sql}#{default_sql}#{unique_sql}#{auto_increment_sql}")
    }
  end

  def define("datetime"), do: define(%{"type" => "datetime"})

  def define(%{"type" => "datetime"} = config) do
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
      "type" => "datetime",
      "default" => default,
      "auto_increment" => auto_increment,
      "is_primary_key" => is_primary_key,
      "required" => "",
      "unique" => unique,
      "size" => 60,
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

  def define("txt"), do: define(%{"type" => "text"})

  def define(%{"type" => "txt"} = config), do: define(%{config | "type" => "text"})

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

  def define("int " <> short_hand), do: define("short_hand", "integer", short_hand)
  def define("dec " <> short_hand), do: define("short_hand", "decimal", short_hand)
  def define("flt " <> short_hand), do: define("short_hand", "float", short_hand)
  def define("flot " <> short_hand), do: define("short_hand", "float", short_hand)
  def define("str " <> short_hand), do: define("short_hand", "string", short_hand)
  def define("vc " <> short_hand), do: define("short_hand", "string", short_hand)
  def define("bool " <> short_hand), do: define("short_hand", "boolean", short_hand)
  def define("bol " <> short_hand), do: define("short_hand", "boolean", short_hand)
  def define("date " <> short_hand), do: define("short_hand", "date", short_hand)
  def define("dte " <> short_hand), do: define("short_hand", "date", short_hand)
  def define("txt " <> short_hand), do: define("short_hand", "text", short_hand)
  def define("text " <> short_hand), do: define("short_hand", "text", short_hand)
  def define("blob " <> short_hand), do: define("short_hand", "blob", short_hand)

  def define(probably_a_value) do
    conf = probably_a_value |> value_to_col_type() |> define()
    {conf, false}
  end

  def define("short_hand", type, short_hand) do
    config = %{
      "type" => type,
      "auto_increment" => false,
      "is_primary_key" => false,
      "required" => false,
      "unique" => false
    }

    [parts_str | t] = String.split(short_hand, " def ")
    parts = String.split(parts_str, " ")
    config = if t == [], do: config, else: Map.put(config, "default", t |> hd() |> String.trim())

    config =
      Enum.reduce(parts, config, fn part, acc_conf ->
        part = String.trim(part)

        cond do
          part in ["unq", "uni", "un", "u"] ->
            Map.put(acc_conf, "unique", true)

          part in ["req", "rq", "r", "nn"] ->
            Map.put(acc_conf, "required", true)

          part in ["null", "n"] ->
            Map.put(acc_conf, "required", false)

          part in ["pk", "p"] ->
            Map.put(acc_conf, "is_primary_key", true)

          part in ["ai", "inc", "auto"] ->
            Map.put(acc_conf, "auto_increment", true)

          true ->
            with {size, _} <- Integer.parse(part) do
              Map.put(acc_conf, "size", size)
            else
              _ -> acc_conf
            end
        end
      end)

    define(config)
  end

  @spec define_columnx(map, <<_::128>>, list) :: %{optional(<<_::24, _::_*24>>) => binary | %{}}
  def define_columnx(%{} = context, "use_primary_keys", list) when is_list(list) do
    sql = if length(list) > 0, do: "PRIMARY KEY(#{Enum.join(list, ", ")})", else: ""

    %{
      "sql" => sql,
      "config" => %{},
      "is_def_only" => true
    }
  end

  def define_column(context, column_name, type) do
    # the context is important incase of different database types
    # using different syntax or incase of a config that may specify different things
    type =
      if type == "fk" do
        # we need to auto infer the parent table using the column_name
        # the expected pattern would be foreign_table_name_id => "pk"
        parent_table_name = String.replace(column_name, "_id", "")
        plural_parent_table_name = Inflex.pluralize(parent_table_name)
        # scan the schema for the primary key
        parent_pk_column_name =
          with schema <- Map.get(context, "schema", %{}),
               parent_table_def <- Map.get(schema, plural_parent_table_name, %{}) do
            Enum.reduce(parent_table_def, "", fn {column_name, config}, acc ->
              is_pk = is_pk_column(config)

              case is_pk do
                true -> column_name
                false -> acc
              end
            end)
          else
            _ -> "id"
          end

        %{
          "fk" => parent_table_name,
          "on" => parent_pk_column_name,
          "on_delete" => "cascade"
        }
      else
        type
      end

    definition = define(type)

    {config, is_def_only} =
      case definition do
        {config, false} -> {config, false}
        _ -> {definition, true}
      end

    column_name = if is_atom(column_name), do: Atom.to_string(column_name), else: column_name

    %{
      "sql" => "#{column_name} #{config["sql"]}",
      "config" => config,
      "is_def_only" => is_def_only
    }
  end

  def is_pk_column("pk"), do: true
  def is_pk_column(%{"is_primary_key" => true} = _config), do: true

  def is_pk_column(%{"sql" => sql}) do
    sql |> String.contains?("PRIMARY KEY")
  end

  def is_pk_column(_config), do: false

  def gen_sql_columns(context, plural_table_name, query_config) do
    # checks to see if the columns in the query_config are all inside of the schema
    temp_results =
      if Map.has_key?(query_config, "columns") == true do
        acc = %{
          "sql" => "",
          "table_schema" => context["schema"][plural_table_name],
          "errors" => %{},
          "context" => context
        }

        Enum.reduce(query_config["columns"], acc, fn {column_name_key, column_config}, acc ->
          skips = Utils.skip_keys()

          case column_name_key in skips do
            false ->
              sql_acc = String.trim(acc["sql"])
              table_schema = acc["table_schema"]

              if Map.has_key?(table_schema, column_name_key) == false do
                # we need to detect for foreign key situations
                if is_propbably_ajoin_term(column_config) do
                  Table.process_join_table(
                    acc,
                    sql_acc,
                    table_schema,
                    plural_table_name,
                    column_name_key,
                    column_config
                  )
                else
                  col_def = define_column(acc["context"], column_name_key, column_config)
                  # update the schema
                  schema = Map.put(table_schema, column_name_key, col_def["config"])
                  comma = if sql_acc == "", do: "", else: ", "
                  sql = sql_acc <> comma <> "ADD " <> String.trim(col_def["sql"])

                  %{
                    "sql" => sql,
                    "table_schema" => schema,
                    "errors" => acc["errors"],
                    "context" => context
                  }
                end
              else
                # add an error in case we are in the add situation
                # nyd: detect if we are in the add situation
                msg = "Column " <> column_name_key <> ", already exists"
                errors = Map.put(acc["errors"], column_name_key, msg)

                %{
                  "sql" => sql_acc,
                  "table_schema" => table_schema,
                  "errors" => errors,
                  "context" => acc["context"]
                }
              end

            true ->
              acc
          end
        end)
      else
        %{
          "sql" => "",
          "table_schema" => %{},
          "errors" => %{},
          "context" => context
        }
      end

    %{
      "table_schema" => table_schema,
      "sql" => foreign_keys_sql
    } =
      if Map.has_key?(query_config, "use_foreign_keys") == true do
        accumulata = %{
          "table_schema" => temp_results["table_schema"],
          "sql" => ""
        }

        Enum.reduce(query_config["use_foreign_keys"], accumulata, fn {column_name_key,
                                                                      foreign_key_config},
                                                                     acc ->
          skips = Utils.skip_keys()

          case column_name_key in skips do
            false ->
              sql_acc = String.trim(acc["sql"])
              # nyd: we need to check if columns already exist
              comma = if sql_acc == "", do: "", else: ", "
              linked_column_name = foreign_key_config["on"]
              plural_parent_table_name = foreign_key_config["fk"] |> Inflex.pluralize()

              on_delete_sql =
                if foreign_key_config["on_delete"] in [nil, "null", "NULL"],
                  do: " SET NULL",
                  else: " CASCADE"

              # change column_name_key in the schema to a foreign key
              fk_def = define(foreign_key_config)
              updated_schema = Map.put(acc["table_schema"], column_name_key, fk_def)

              temp_sql =
                "#{sql_acc}#{comma}ADD FOREIGN KEY(#{column_name_key}) REFERENCES #{plural_parent_table_name}(#{linked_column_name}) ON DELETE#{on_delete_sql}"

              %{"table_schema" => updated_schema, "sql" => temp_sql}

            true ->
              acc
          end
        end)
      else
        %{
          "table_schema" => temp_results["table_schema"],
          "sql" => ""
        }
      end

    temp_results_sql = temp_results["sql"]

    comma =
      cond do
        temp_results_sql == "" && foreign_keys_sql == "" -> ""
        temp_results_sql != "" && foreign_keys_sql == "" -> ""
        temp_results_sql == "" && foreign_keys_sql != "" -> ""
        temp_results_sql != "" && foreign_keys_sql != "" -> ", "
        true -> ""
      end

    %{
      temp_results
      | "sql" => "#{temp_results_sql}#{comma}#{foreign_keys_sql}",
        "table_schema" => table_schema
    }
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
    item_to_scan = if is_list(item_to_scan), do: item_to_scan, else: query_config
    # Utils.log("query_config", item_to_scan, is_list(item_to_scan))
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

  def sql_value_format(nil), do: "NULL"
  def sql_value_format(value) when is_binary(value), do: "'#{value}'"

  def sql_value_format(value) when is_map(value),
    do: throw("Error!, sql value format for map is not yet implemented")

  def sql_value_format(value), do: "#{value}"

  def sql_column_name(context, table_name, column_name) do
    plural_table_name = Inflex.pluralize(table_name)
    "`#{context["database_name"]}.#{plural_table_name}.#{column_name}`"
  end

  def process_foreign_keys(table_col_def, query_config) do
    if Map.has_key?(query_config, "use_foreign_keys") == true do
      accumulata = %{
        "table_schema" => table_col_def["table_schema"],
        "sql" => ""
      }

      Enum.reduce(query_config["use_foreign_keys"], accumulata, fn {column_name_key,
                                                                    foreign_key_config},
                                                                   acc ->
        skips = Utils.skip_keys()

        case column_name_key in skips do
          false ->
            sql_acc = String.trim(acc["sql"])
            # nyd: we need to check if columns already exist
            comma = if sql_acc == "", do: "", else: ", "
            linked_column_name = foreign_key_config["on"]
            plural_parent_table_name = foreign_key_config["fk"] |> Inflex.pluralize()

            on_delete_sql =
              if foreign_key_config["on_delete"] in [nil, "null", "NULL"],
                do: " SET NULL",
                else: " CASCADE"

            # change column_name_key in the schema to a foreign key
            fk_def = define(foreign_key_config)
            updated_schema = Map.put(acc["table_schema"], column_name_key, fk_def)

            temp_sql =
              "#{sql_acc}#{comma}ADD FOREIGN KEY(#{column_name_key}) REFERENCES #{plural_parent_table_name}(#{linked_column_name}) ON DELETE#{on_delete_sql}"

            %{"table_schema" => updated_schema, "sql" => temp_sql}

          true ->
            acc
        end
      end)
    else
      %{
        "table_schema" => table_col_def["table_schema"],
        "sql" => ""
      }
    end
  end

  def is_data_type(term) do
    data_types = [
      "fk",
      "pk",
      "integer",
      "int",
      "bool",
      "boolean",
      "str",
      "string",
      "timestamp",
      "date",
      "datetime",
      "float",
      "decimal",
      "blob",
      "txt",
      "text"
    ]

    cond do
      is_map(term) && Map.has_key?(term, "type") && term["type"] in data_types -> true
      is_binary(term) && term in data_types -> true
      true -> false
    end
  end

  def is_propbably_ajoin_term(term) do
    is_data_type(term) == false &&
      (is_map(term) ||
         is_list(term) ||
         (is_map(term) && Map.has_key?(term, "dao@link")) ||
         (is_map(term) && Map.has_key?(term, "dao@join"))) &&
      (is_map(term) &&
         Map.has_key?(term, "fk") == false) &&
      (is_map(term) &&
         Map.has_key?(term, "as") == false)
  end

  def get_primary_keys(table_schema) do
    Enum.reduce(table_schema, [], fn {parent_col_name, parent_col_config}, acc ->
      cond do
        is_map(parent_col_config) && Map.has_key?(parent_col_config, "is_primary_key") &&
            parent_col_config["is_primary_key"] == true ->
          acc ++ [parent_col_name]

        is_binary(parent_col_config) && parent_col_config == "pk" ->
          acc ++ [parent_col_name]

        true ->
          acc
      end
    end)
  end

  def get_foreign_key_config(table_schema, parent_table_name) do
    parent_plural_table_name = Inflex.pluralize(parent_table_name)

    Enum.reduce_while(table_schema, {nil, nil}, fn {col_name, col_config}, acc ->
      cond do
        is_map(col_config) && Map.has_key?(col_config, "fk") &&
            col_config["fk"] == parent_plural_table_name ->
          {:halt, {col_name, col_config}}

        is_binary(col_config) && col_config == "fk" ->
          plural_col_name = String.trim_trailing(col_name, "_id") |> Inflex.pluralize()

          if plural_col_name == parent_plural_table_name do
            def_col_config =
              define(%{
                "fk" => plural_col_name,
                "on" => "id",
                "on_delete" => "cascade"
              })

            {:halt, {col_name, def_col_config}}
          else
            {:cont, acc}
          end

        true ->
          {:cont, acc}
      end
    end)
  end
end
