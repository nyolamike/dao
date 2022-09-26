defmodule Dbms do
  def query(context, sql) do
    query(context, nil, sql)
  end

  def query(context, connection, sql) do
    db_type = context["database_type"]

    case db_type do
      "mysql" -> run_mysql_query(context, connection, sql)
      true -> throw("Unknown/missing dbms specified in context")
    end
  end

  def connect(context) do
    db_name = context["database_name"]
    db_type = context["database_type"]

    case db_type do
      "mysql" -> run_mysql_connect_to_db(context, db_name)
      true -> throw("Unknown/missing dbms specified in context while establishing a connection")
    end
  end

  def run_mysql_connect_to_db(_context, db_name) do
    # nyd: again these need to picked form an env config setup
    with {:ok, connection_pid} <-
           MyXQL.start_link(username: "root", password: "Mysql@2015", database: db_name) do
      connection_pid
    else
      err ->
        # nyd: propabily this need to be logged
        IO.inspect(err)
        raise "Failed to establish database connection to mysql db "
    end
  end

  def run_mysql_query(_context, connection_pid, sql) do
    case connection_pid do
      nil -> MyXQL.query(:myxql, sql)
      _ -> MyXQL.query(connection_pid, sql)
    end
  end
end
