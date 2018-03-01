using JSON

include("pgbench_julia.jl")

function main()
    query_file, user, _ = ARGS
    duration = parse(Int, ARGS[3])
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

main()
