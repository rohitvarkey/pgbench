using LibPQ
using JSON
using DataStreams, NamedTuples

function executor(conn, query, query_args)
    result = execute(conn, query, query_args, throw_error=true)
    data = Data.stream!(result, NamedTuple)
    rows = num_affected_rows(result)
    clear!(result)
    return rows
end

function runner(conn, query, query_args, duration)
    start = time()
    queries = 0
    rows = 0
    while time() - start < duration
        rows += executor(conn, query, query_args)
        queries += 1
    end
    return queries, rows, time() - start
end


function run(query_file, user, duration)
    conn = LibPQ.Connection("dbname=postgres user=$user")
    json = JSON.parsefile(query_file)
    @show json["query"] , json["args"]
    if "teardown" in keys(json)
        result = execute(conn, json["teardown"])
        clear!(result)
    end
    if "setup" in keys(json)
        result = execute(conn, json["setup"])
        clear!(result)
    end
    #precompile and warmup
    runner(conn, json["query"] , json["args"], 5)
    queries, rows, timetaken = runner(conn, json["query"], json["args"], duration)
    if "teardown" in keys(json)
        result = execute(conn, json["teardown"])
        clear!(result)
    end
    close(conn)
    println("$queries queries performed returning $rows rows in $timetaken")
    println("Queries/sec: $(queries/timetaken)")
    println("Rows/sec: $(rows/timetaken)")
    queries, rows, timetaken
end
