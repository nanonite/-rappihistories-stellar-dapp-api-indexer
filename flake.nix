{
  description = "Medichain API indexer toolchain";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  };

  outputs =
    { nixpkgs
    , ...
    }:
    let
      supportedSystems = [
        "x86_64-linux"
        "aarch64-linux"
      ];

      forAllSystems = nixpkgs.lib.genAttrs supportedSystems;

      pkgsFor = system:
        import nixpkgs {
          inherit system;
        };

      apiIndexerShellFor = system:
        let
          pkgs = pkgsFor system;
        in
        pkgs.mkShell {
          name = "medichain-api-indexer";

          packages = with pkgs; [
            bashInteractive
            bun
            cacert
            git
            jq
            just
            nixpkgs-fmt
            nodejs_22
            postgresql_16
            typescript
          ];

          DATABASE_URL = "postgres://medichain:medichain@localhost:5432/medichain";
          STELLAR_RPC_URL = "http://localhost:8000/soroban/rpc";
          API_INDEXER_PORT = "8788";

          shellHook = ''
            echo "Medichain API indexer: Node $(node --version), Bun $(bun --version), TypeScript $(tsc --version | cut -d' ' -f2), Postgres $(postgres --version | cut -d' ' -f3)"
            echo "MVP env: DATABASE_URL=$DATABASE_URL STELLAR_RPC_URL=$STELLAR_RPC_URL API_INDEXER_PORT=$API_INDEXER_PORT"
          '';
        };
    in
    {
      devShells = forAllSystems
        (system: {
          default = apiIndexerShellFor system;
          ci = apiIndexerShellFor system;
        });

      checks = forAllSystems
        (system:
          let
            pkgs = pkgsFor system;
          in
          {
            api-indexer-schema = pkgs.runCommand "medichain-api-indexer-schema"
              {
                nativeBuildInputs = [
                  pkgs.coreutils
                  pkgs.gnugrep
                  pkgs.postgresql_16
                ];
                src = ./.;
              } ''
              cp -R "$src" ./api-indexer
              chmod -R u+w ./api-indexer

              export PGDATA="$PWD/postgres-data"
              export PGHOST="$PWD/postgres-socket"
              export PGDATABASE=medichain
              export PGUSER=medichain
              export LOG_PATH="$PWD/postgres.log"

              mkdir -p "$PGHOST"
              initdb --auth=trust --username="$PGUSER" --no-locale --encoding=UTF8 > /dev/null
              pg_ctl -w -l "$LOG_PATH" -o "-k $PGHOST" start
              trap 'pg_ctl -w stop' EXIT

              psql --no-psqlrc --set=ON_ERROR_STOP=1 --dbname=postgres --command="CREATE DATABASE medichain;"
              psql --no-psqlrc --set=ON_ERROR_STOP=1 --file=./api-indexer/src/storage/migrations/001_initial.sql

              for table in grants records audit_events prescriptions inventory_units credentials notifications _indexer_state; do
                psql --no-psqlrc --tuples-only --command="SELECT to_regclass('public.$table') IS NOT NULL;" | grep -q t
              done

              psql --no-psqlrc --tuples-only --command="SELECT value FROM _indexer_state WHERE key = 'last_ledger';" | grep -q 0

              mkdir -p "$out"
            '';
          });

      formatter = forAllSystems (system: (pkgsFor system).nixpkgs-fmt);
    };
}
