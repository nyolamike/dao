defmodule DaoEngine.Application do
  use Application

  def start(_type, _args) do
    # nyd: username and password should be comming from a .env file and configs
    children = [
      {MyXQL, username: "root", name: :myxql, password: "Mysql@2015"}
    ]

    opts = [strategy: :one_for_one, name: DaoEngine.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
