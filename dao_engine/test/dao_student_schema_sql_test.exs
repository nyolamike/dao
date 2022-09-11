defmodule DaoStudentSchemaSqlTest do
  use ExUnit.Case
  doctest DaoEngine

  alias DaoEngine, as: Dao

  test "reads in schema convert student table to create sql " do
    with {:ok, context} <- Dao.load_config_from_file("company_book") do
      expected_results = expected_schema_results()
      expected_auto_changes_results = expeted_changes_sql()

      ensure_table_response = Database.gen_sql_ensure_database_exists(context)

      {modified_context, _any_alter_table_sql, _is_def_only} =
        case ensure_table_response do
          {modified_context, sql} -> {modified_context, sql, true}
          {modified_context, sql, is_def_only} -> {modified_context, sql, is_def_only}
          modified_context -> {modified_context, "", true}
        end

      assert expected_results == modified_context["schema"]
      assert expected_auto_changes_results == modified_context["auto_schema_changes"]
    else
      _ -> throw("Faild to read data dao config file ")
    end
  end

  def expected_schema_results() do
    %{
      "students" => %{
        "major" => %{
          "auto_increment" => false,
          "default" => "",
          "is_primary_key" => false,
          "required" => false,
          "size" => 30,
          "sql" => "VARCHAR(30)",
          "type" => "string",
          "unique" => false
        },
        "name" => %{
          "auto_increment" => false,
          "default" => "",
          "is_primary_key" => false,
          "required" => false,
          "size" => 30,
          "sql" => "VARCHAR(30)",
          "type" => "string",
          "unique" => false
        },
        "student_id" => %{
          "auto_increment" => true,
          "default" => nil,
          "is_primary_key" => true,
          "required" => false,
          "size" => 30,
          "sql" => "INT(30) AUTO_INCREMENT NOT NULL PRIMARY KEY",
          "type" => "integer"
        }
      }
    }
  end

  def expeted_changes_sql() do
    [
      "CREATE TABLE `company_book.students` (major VARCHAR(30), name VARCHAR(30), student_id INT(30) AUTO_INCREMENT NOT NULL PRIMARY KEY)"
    ]
  end

  test "primary key word as sperate line in command " do
    with {:ok, context} <- Dao.load_config_from_file("company_book_v_1_1") do
      expected_results = expected_schema_v_1_1_results()
      expected_auto_changes_results = expeted_changes_v_1_1_sql()

      ensure_table_response = Database.gen_sql_ensure_database_exists(context)

      {modified_context, _any_alter_table_sql, _is_def_only} =
        case ensure_table_response do
          {modified_context, sql} -> {modified_context, sql, true}
          {modified_context, sql, is_def_only} -> {modified_context, sql, is_def_only}
          modified_context -> {modified_context, "", true}
        end

      assert expected_results == modified_context["schema"]
      assert expected_auto_changes_results == modified_context["auto_schema_changes"]
    else
      _ -> throw("Faild to read data dao config file ")
    end
  end

  def expected_schema_v_1_1_results() do
    %{
      "students" => %{
        "major" => %{
          "auto_increment" => false,
          "default" => "",
          "is_primary_key" => false,
          "required" => false,
          "size" => 30,
          "sql" => "VARCHAR(30)",
          "type" => "string",
          "unique" => false
        },
        "name" => %{
          "auto_increment" => false,
          "default" => "",
          "is_primary_key" => false,
          "required" => false,
          "size" => 30,
          "sql" => "VARCHAR(30)",
          "type" => "string",
          "unique" => false
        },
        "student_id" => %{
          "auto_increment" => false,
          "default" => "",
          "is_primary_key" => false,
          "required" => false,
          "size" => 30,
          "sql" => "INT(30)",
          "type" => "integer",
          "unique" => false
        }
      }
    }
  end

  def expeted_changes_v_1_1_sql() do
    [
      "CREATE TABLE `company_book.students` (major VARCHAR(30), name VARCHAR(30), student_id INT(30), PRIMARY KEY(student_id))"
    ]
  end
end
