using System;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using InfluxDB.Client;
using InfluxDB.Client.Core;
using InfluxDB.Client.Writes;
using System.Linq;
using System.Linq.Expressions;
using System.Timers;
using System.Resources;
using NodaTime;
using InfluxDB.Client.Api.Domain;
using System.Globalization;


public class Atom
{
    // Properties of an Atom
    public string Name { get; set; }
    public string Alias { get; set; }
    public DateTime starting { get; set; }
    public DateTime ending { get; set; }
    public string CycleTime { get; set; }
    public int LastCycleTime { get; set; }

    // Constructor to initialize Atom with a name and alias
    public Atom(string name, string alias)
    {
        Name = name;
        Alias = alias;
    }
}

class SocketClient
{
    // Constants for server communication
    private const int SendingPort = 7122;  // Port for sending messages
    private const int ListeningPort = 8122; // Port for receiving messages
    private const string ServerIp = "127.0.0.1"; // IP address of the server

    // Enum representing the states of the system
    public enum State
    {
        IDLE,
        EXECUTE,
        COMPLETE
    }

    // Static variables for managing state and system data
    public static State state = State.IDLE; // Current state
    public static State laststate; // Previous state
    static bool runs = false; // Flag for controlling message sending
    static List<Atom> atoms = new List<Atom>(); // List of Atom objects
    static string[] atomname = new string[50]; // Temporary storage for atom names
    static string[] ats = new string[50]; // Temporary storage for atom details
    static string queryStartTime = "1970-01-01T00:00:00Z"; // Start time for database queries
    static TimeSpan periodicInterval = TimeSpan.FromSeconds(60); // Interval for periodic tasks
    static string[] parts = new string[50]; // Temporary storage for received parts
    static bool NewData = false; // Flag indicating if new data is available
    static string[] cycles = {
        "Normal(125.391741472172, 0.041944774841062)", "Normal(26.2311490409099, 0.316524145459864)",
        "lognormal(8.35047881459711, 5.78730821141269)", "Normal(32.2400214141414, 0.0341634185278404)",
        "Normal(81.6964458670989, 32.1075089282547)", "Normal(130.669964906124, 0.283933896401368)",
        "Normal(2.16361834727457, 0.0134763225110203)", "Normal(3.0356335978836, 0.0231661249631185)",
        "Normal(11.5756640638556, 0.377962844432547)", "Normal(281.532260403863, 3.43632205152574)",
        "Normal(281.532260403863, 3.43632205152574)", "Normal(281.532260403863, 3.43632205152574)",
        "Normal(84.8406583212967, 25.5751462500916)"
    };

    // Main method - Entry point of the program
    static async Task Main(string[] args)
    {
        // Create and start threads for different tasks
        Thread DbExtractThread = new Thread(DbExtract);
        Thread sendThread = new Thread(StartSender);
        Thread receiveThread = new Thread(StartReceiver);
        Thread stateUpdateThread = new Thread(StateUpdater);

        sendThread.Start();
        receiveThread.Start();
        DbExtractThread.Start();
        stateUpdateThread.Start();

        // Wait for threads to complete
        sendThread.Join();
        receiveThread.Join();
        DbExtractThread.Join();
        stateUpdateThread.Join();
    }

    // Thread function for extracting data from the database
    async static void DbExtract()
    {
        // Retrieve the database token and initialize the InfluxDB client
        // Update token, org and client as needed.
        string token = Environment.GetEnvironmentVariable("INFLUXDB_TOKEN")!;
        const string org = "OMRON";
        using var client = new InfluxDBClient("http://192.168.1.62:8086", "Lx37wg69CCoNulSyQl4vh1yWYCHZ0fCRv76eZvM6lk4KjBnydfIx5eil4gVe6n8V6sln1tT1q7sblqChXeLDgg==");

        while (true)
        {
            // Perform actions based on the current state
            switch (state)
            {
                case State.EXECUTE:
                    // Execute database queries for each atom in the EXECUTE phase
                    for (var i = 0; i < atoms.Count(); i++)
                    {
                        // Formulate query for the EXECUTE phase
                        var query = $@"from(bucket: ""OMRON_BUCKET"")
            |> range(start:{queryStartTime}, stop: now())
            |> filter(fn: (r) => r[""_measurement""] == ""machine_status"")
            |> filter(fn: (r) => r[""_field""] == ""StateCurrent"")
            |> filter(fn: (r) => r[""_value""] == ""{State.EXECUTE.ToString()}"")
            |> filter(fn: (r) => r[""name""] == ""{atoms[i].Alias}"")";

                        // Execute the query and process results
                        var tables = await client.GetQueryApi().QueryAsync(query, org);
                        if (!tables.Any(table => table.Records.Any()))
                        {
                            Console.WriteLine($"No new data for Atom: {atoms[i].Alias} in EXECUTE phase.");
                            NewData = false;
                            continue;
                        }
                        else
                        {
                            NewData = true;
                            foreach (var record in tables.SelectMany(table => table.Records))
                            {
                                atoms[i].starting = (DateTime)record.GetTimeInDateTime();
                                Console.WriteLine($"Starting time: {atoms[i].starting}");
                            }
                        }
                    }
                    state = State.COMPLETE;
                    break;

                case State.COMPLETE:
                    // Execute database queries for each atom in the COMPLETE phase
                    for (var i = 0; i < atoms.Count(); i++)
                    {
                        // Formulate query for the COMPLETE phase
                        var query = $@"from(bucket: ""OMRON_BUCKET"")
            |> range(start:{queryStartTime}, stop: now())
            |> filter(fn: (r) => r[""_measurement""] == ""machine_status"")
             |> filter(fn: (r) => r[""_field""] == ""StateCurrent"")
            |> filter(fn: (r) => r[""_value""] == ""{State.COMPLETE.ToString()}"")
            |> filter(fn: (r) => r[""name""] == ""{atoms[i].Alias}"")";
                        var tables = await client.GetQueryApi().QueryAsync(query, org);

                        if (!tables.Any(table => table.Records.Any()))
                        {
                            Console.WriteLine($"No new data for Atom: {atoms[i].Alias} in COMPLETE phase.");
                            NewData = false;
                            continue;
                        }
                        else
                        {
                            NewData = true;
                            foreach (var record in tables.SelectMany(table => table.Records))
                            {
                                atoms[i].ending = (DateTime)record.GetTimeInDateTime();
                                Console.WriteLine($"Ending time: {atoms[i].ending}");
                            }
                        }
                    }

                    laststate = State.COMPLETE;
                    state = State.IDLE;
                    queryStartTime = atoms[atoms.Count() - 1].ending.AddSeconds(1).ToString("yyyy-MM-ddTHH:mm:ssZ");
                    break;
            }

            // Calculate cycle times if data is available
            if (atoms.Count > 0 && state == State.IDLE && laststate == State.COMPLETE && NewData == true)
            {
                for (int i = 0; i < atoms.Count(); i++)
                {
                    TimeSpan difference = atoms[i].ending - atoms[i].starting;
                    atoms[i].CycleTime = difference.Seconds.ToString();
                    Console.WriteLine($"Atom: {atoms[i].Name} CycleTime: {atoms[i].CycleTime}");
                }
                laststate = State.IDLE;
                runs = true;
            }
        }
    }

    // Helper method to validate cycle times for atoms
    static bool ValidateCycleTimes(List<Atom> atomList)
    {
        foreach (var atom in atomList)
        {
            if (atom.CycleTime == "0")
            {
                Console.WriteLine($"Atom {atom.Name} does not have a valid cycle time.");
                return false;
            }
        }
        return true;
    }

    // Thread function to update the state periodically
    static void StateUpdater()
    {
        while (true)
        {
            if (state == State.IDLE && atoms.Count() > 0)
            {
                Console.WriteLine("Switching state to EXECUTE...");
                state = State.EXECUTE;
            }
            Thread.Sleep(periodicInterval); // Wait before checking again
        }
    }

    // Thread function to send messages to the server
    static void StartSender()
    {
        TcpClient senderClient = new TcpClient();

        try
        {
            // Connect to the server
            senderClient.Connect(IPAddress.Parse(ServerIp), SendingPort);
            Console.WriteLine($"Sender connected to server on {ServerIp}:{SendingPort}");

            NetworkStream stream = senderClient.GetStream();

            // Send an initial handshake message
            string initialMessage = "C#Hello";
            SendMessage(stream, initialMessage);
            Console.WriteLine($"Sent initial message: {initialMessage}");

            while (true)
            {
                if (runs) // Check if it's time to send messages
                {
                    if (ValidateCycleTimes(atoms))
                    {
                        // Send cycle time messages for each atom
                        for (int i = 0; i < atoms.Count; i++)
                        {
                            string message = $"C#Do(SetExprAtt([CycleTime],[{atoms[i].CycleTime}],refto{atoms[i].Name}))";
                            SendMessage(stream, message);
                            Console.WriteLine($"Sent: {message}");
                            Thread.Sleep(500); // Delay between messages
                        }
                        runs = false; // Stop sending messages
                    }
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine("Error in Sender: " + ex.Message);
        }
    }

    // Thread function to receive messages from the server
    static void StartReceiver()
    {
        TcpListener listener = new TcpListener(IPAddress.Any, ListeningPort);

        try
        {
            // Start listening for incoming connections
            listener.Start();
            Console.WriteLine($"Receiver listening on port {ListeningPort}");

            while (true)
            {
                TcpClient client = listener.AcceptTcpClient();
                Console.WriteLine("Client connected for listening.");

                NetworkStream stream = client.GetStream();
                byte[] buffer = new byte[1024];

                while (true)
                {
                    int bytesRead = stream.Read(buffer, 0, buffer.Length);
                    if (bytesRead == 0) // Check if the connection was closed
                    {
                        Console.WriteLine("Connection closed by sender.");
                        break;
                    }

                    string receivedMessage = Encoding.ASCII.GetString(buffer, 0, bytesRead);
                    Console.WriteLine($"Received: {receivedMessage}");

                    // Handle specific types of messages
                    if (receivedMessage == "HelloACK")
                    {
                        Console.WriteLine("Handshake complete. Activating message sending.");
                    }
                    else if (receivedMessage.Contains("Atoms:"))
                    {
                        // Parse received atom data
                        parts = receivedMessage.Split(',');
                        for (int i = 0; i < parts.Length; i++)
                        {
                            ats = parts[i].Split(":");
                            if (ats.Length == 2)
                            {
                                atomname = ats[1].Split(';');
                                if (atomname.Length == 2)
                                {
                                    atomname[0] = atomname[0].Replace(" ", "");
                                    Atom atom = new Atom(atomname[0], atomname[1]);
                                    atoms.Add(atom);
                                    Console.WriteLine($"Atom ={atoms[i].Name}  Alias = {atoms[i].Alias}");
                                }
                            }
                        }
                    }
                    else if (receivedMessage == "AtomListSent")
                    {
                        Console.WriteLine("Atom list done");
                        state = State.EXECUTE;
                    }
                }

                client.Close();
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine("Error in Receiver: " + ex.Message);
        }
        finally
        {
            listener.Stop();
        }
    }

    // Helper method to send a message over a network stream
    static void SendMessage(NetworkStream stream, string message)
    {
        byte[] messageBytes = Encoding.UTF8.GetBytes(message);
        stream.Write(messageBytes, 0, messageBytes.Length);
    }
}

