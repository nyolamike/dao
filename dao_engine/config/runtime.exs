import Config

DotenvParser.load_file(".env")

# Now variables from `.env` are loaded into system env
config :dao_engine,
  base_root_file_path: System.fetch_env!("BASE_ROOT_FILE_PATH")
