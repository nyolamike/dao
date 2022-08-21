defmodule Column do
  def define("pk") do
    %{
      "type" => "integer",
      "size" => 30,
      "default" => nil,
      "auto_increment" => true,
      "is_primary_key" => true,
      "required" => false,
      "sql" => "INT(30) PRIMARY KEY"
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
    default_sql = if default != nil, do: "DEFAULT #{default}", else: "DEFAULT 0"
    # default is true, meaning it does not allow nil/null values
    required = if Map.has_key?(config, "required"), do: config["required"], else: true
    required_sql = if required == true, do: "NOT NULL", else: ""

    %{
      "type" => "string",
      "size" => size,
      "default" => default,
      "auto_increment" => false,
      "is_primary_key" => is_primary_key,
      "required" => required,
      "sql" => "TINYINT(#{size}) #{required_sql} #{default_sql}"
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
    default_sql = if default != "", do: "DEFAULT '#{default}'", else: ""
    # default is false, meaning it allows nil/null values
    required = if Map.has_key?(config, "required"), do: config["required"], else: false
    required_sql = if required == true, do: "NOT NULL", else: ""

    %{
      "type" => "string",
      "size" => size,
      "default" => default,
      "auto_increment" => false,
      "is_primary_key" => is_primary_key,
      "required" => required,
      "sql" => "VARCHAR (#{size}) #{required_sql} #{default_sql}"
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
    default_sql = if default != nil, do: "DEFAULT #{default}", else: "DEFAULT (UNIX_TIMESTAMP())"
    # default is false, meaning it allows nil/null values
    required = if Map.has_key?(config, "required"), do: config["required"], else: false
    required_sql = if required == true, do: "NOT NULL", else: ""

    %{
      "type" => "string",
      "size" => size,
      "default" => default,
      "auto_increment" => false,
      "is_primary_key" => is_primary_key,
      "required" => required,
      "sql" => "BIGINT (#{size}) #{required_sql} #{default_sql}"
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

    default_sql = if default != nil, do: "DEFAULT #{default}", else: "DEFAULT CURRENT_TIMESTAMP"

    # default is false, meaning it allows nil/null values
    required = if Map.has_key?(config, "required"), do: config["required"], else: false
    required_sql = if required == true, do: "NOT NULL", else: ""

    %{
      "type" => "datetime",
      "size" => size,
      "default" => default,
      "auto_increment" => false,
      "is_primary_key" => is_primary_key,
      "required" => "",
      "sql" => "DATETIME #{required_sql} #{default_sql}"
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
end
