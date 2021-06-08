using System;
using System.Collections.Generic;
using System.Linq;
using Dapper;
using Npgsql;

namespace CleanPostgreSQLDatabase
{
    class Program
    {
        private const string Hostname = "localhost";
        private const string Database = "postgres";
        private const string Username = "postgres";
        private const string ConnectionString = "Host=" + Hostname + ";Database=" + Database + ";Username=" + Username;

        private static NpgsqlConnection Connection { get; set; } = null;
        private static bool _closeRequested = false;
        static void Main(string[] args)
        {
            while (!_closeRequested)
            {
                try
                {
                    PrintMenu();
                    EnsureConnectionOpen();

                    var tables = ReadAllTables();
                    PrintTables(tables);

                    var command = Console.ReadLine();
                    InterpretCommand(command, tables);
                }
                catch (Exception exc)
                {
                    CloseConnection();
                    WriteError(exc);
                }
            }
        }

        private static void PrintMenu()
        {
            Console.Clear();
            Console.WriteLine("Clean all tables util...");
            Console.WriteLine("________________________");
            Console.WriteLine();
            Console.WriteLine("r ... refresh");
            Console.WriteLine("s ... show data");
            Console.WriteLine("d ... delete");
            Console.WriteLine("t ... truncate");
            Console.WriteLine("q ... quit");
            Console.WriteLine("________________________");
            Console.WriteLine();
        }

        private static void EnsureConnectionOpen()
        {
            if (Connection == null)
            {
                Connection = new NpgsqlConnection(ConnectionString);
                Connection.Open();
            }
        }

        private static void InterpretCommand(string command, List<string> tables)
        {
            if (command == "d")
            {
                RemoveAllTables(tables);
                return;
            }

            if (command == "s")
            {
                ShowAllTables(tables);
                return;
            }

            if (command == "t")
            {
                TruncateTables(tables);
            }
        }

        private static void TruncateTables(List<string> tables)
        {
            tables.ForEach(table => Connection.Execute($"truncate table \"{table}\";"));
        }

        private static void CloseConnection()
        {
            try
            {
                Connection?.Close();
                Connection = null;
            }
            catch (Exception exc)
            {
                Connection = null;
            }
        }

        private static void WriteError(Exception exc, bool waitForUserInput = true)
        {
            Console.WriteLine("Error: " + exc.Message);
            if (waitForUserInput)
            {
                Console.ReadLine();
            }
        }

        private static void ShowAllTables(List<string> tables)
        {
            tables.ForEach(table =>
            {
                Console.WriteLine($"Print: {table}");
                Console.WriteLine("".PadRight(7 + table.Length), '_');
                Console.WriteLine();

                using var reader = Connection.ExecuteReader($"select * from \"{table}\"");

                for (int i = 0; i < reader.FieldCount; i++)
                {
                    Console.Write(reader.GetName(i) + "\t");
                }
                Console.WriteLine();

                while (reader.Read())
                {
                    for (int i = 0; i < reader.FieldCount; i++)
                    {
                        Console.Write(reader.GetValue(i).ToString() + "\t");
                    }

                    Console.WriteLine();
                }

                reader.Close();
            });

            Console.WriteLine("Press any key to continue...");
            Console.ReadLine();
        }

        private static void PrintTables(List<string> tables)
        {
            if (!tables.Any())
            {
                Console.WriteLine("no tables");
                return;
            }

            Console.WriteLine(string.Join(", " + Environment.NewLine, tables));
        }

        private static List<string> ReadAllTables()
        {
            // ReSharper disable StringLiteralTypo

            return
                Connection.Query("select tablename from pg_tables where schemaname = 'public'")
                    .Select(x => x.tablename as string)
                    .ToList();

            // ReSharper restore StringLiteralTypo
        }

        private static void RemoveAllTables(List<string> tables)
        {
            tables.ForEach(table => Connection.Execute($"drop table \"{table}\""));
        }
    }
}
