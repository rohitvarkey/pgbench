using JSON

include("pgbench_julia.jl")

PG_USER = "rohitvarkey"
DURATION = 30

function main(query_file, user, duration)
    queries, rows, timetaken = run(query_file, user, duration)
    open("$(basename(query_file))_results.json", "w") do f
        @show j = json(Dict(
            "Queries" => queries,
            "Rows" => rows,
            "Time" => timetaken,
            "QPS" => queries/timetaken,
            "RPS" => rows/timetaken
        ))
        write(f, j)
    end
end

for query_file in readdir("../queries")
    println("Running query from $(query_file)")
    try
        main(joinpath("../queries", query_file), PG_USER, DURATION)
    catch
        println("Running query $(query_file) failed")
    end
end
