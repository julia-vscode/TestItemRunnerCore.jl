module TestItemRunnerCore

export run_tests, kill_test_processes, terminate_process, get_active_processes,
       RunProfile, ProcessInfo,
       TestrunResult, TestrunResultTestitem, TestrunResultTestitemProfile,
       TestrunResultMessage, TestrunResultStackFrame, TestrunResultDefinitionError,
       TestrunRecord, get_run_history, get_active_runs, cancel_run,
       get_run_result, get_last_run_id,
       TestItemRunner, get_runner

# Re-export CancellationTokens API so consumers don't need to reach into internals
export CancellationTokenSource, CancellationToken, cancel, get_token, is_cancellation_requested

import ProgressMeter, JuliaWorkspaces, AutoHashEquals, TestItemControllers, Logging
import UUIDs
using Query

using JuliaWorkspaces: JuliaWorkspace
using JuliaWorkspaces.URIs2: URI, filepath2uri, uri2filepath
using AutoHashEquals: @auto_hash_equals
using TestItemControllers: TestItemController, ControllerCallbacks
using TestItemControllers.CancellationTokens: CancellationTokenSource, CancellationToken,
    cancel, get_token, is_cancellation_requested

# ── Public types ──────────────────────────────────────────────────────

@auto_hash_equals struct RunProfile
    name::String
    coverage::Bool
    env::Dict{String,Any}
end

struct ProcessInfo
    id::String
    package_name::String
    status::String
end

struct TestrunResultStackFrame
    label::String
    uri::URI
    line::Int
    column::Int
end

struct TestrunResultMessage
    message::String
    expected_output::Union{Nothing,String}
    actual_output::Union{Nothing,String}
    uri::URI
    line::Int
    column::Int
    stack_frames::Union{Nothing,Vector{TestrunResultStackFrame}}
end

struct TestrunResultTestitemProfile
    profile_name::String
    status::Symbol
    duration::Union{Nothing,Float64}
    messages::Union{Nothing,Vector{TestrunResultMessage}}
    output::Union{Nothing,String}
end

struct TestrunResultTestitem
    name::String
    uri::URI
    profiles::Vector{TestrunResultTestitemProfile}
end

struct TestrunResultDefinitionError
    message::String
    uri::URI
    line::Int
    column::Int
end

struct TestrunResult
    definition_errors::Vector{TestrunResultDefinitionError}
    testitems::Vector{TestrunResultTestitem}
    process_outputs::Dict{String,String}
end

# ── Per-run context (keyed by testrun_id) ─────────────────────────────

mutable struct RunContext
    testitems_by_id::Dict{String,TestItemControllers.TestItemDetail}
    environments::Vector{RunProfile}
    environment_name::String
    progress_ui::Symbol
    progressbar_next::Function
    count_success::Int
    count_fail::Int
    count_error::Int
    count_skipped::Int
    n_total::Int
    responses::Vector{Any}
    outputs::Dict{String,Vector{String}}
    launch_header_printed::Bool
end

# ── Run history ───────────────────────────────────────────────────────

mutable struct TestrunRecord
    id::String
    start_time::Float64
    end_time::Union{Nothing,Float64}
    status::Symbol  # :running, :completed, :cancelled, :errored
    result::Union{Nothing,TestrunResult}
    path::String
    cts::Union{Nothing,CancellationTokenSource}
end

# ── Runner state singleton ────────────────────────────────────────────

mutable struct TestItemRunner
    controller::TestItemController
    lock::ReentrantLock
    run_contexts::Dict{String,RunContext}
    processes::Dict{String,ProcessInfo}
    process_outputs::Dict{String,Vector{String}}
    run_history::Vector{TestrunRecord}
    run_counter::Ref{Int}
    max_history::Int
    test_env_by_id::Dict{String,TestItemControllers.TestEnvironment}
end

function TestItemRunner(controller::TestItemController; max_history::Int=20)
    TestItemRunner(
        controller,
        ReentrantLock(),
        Dict{String,RunContext}(),
        Dict{String,ProcessInfo}(),
        Dict{String,Vector{String}}(),
        Vector{TestrunRecord}(),
        Ref(0),
        max_history,
        Dict{String,TestItemControllers.TestEnvironment}(),
    )
end

const _g_runner = Ref{TestItemRunner}()
const _g_runner_lock = ReentrantLock()

function get_run_context(testrun_id::String)
    runner = get_runner()
    lock(runner.lock) do
        get(runner.run_contexts, testrun_id, nothing)
    end
end

function get_active_processes()
    runner = get_runner()
    lock(runner.lock) do
        collect(values(runner.processes))
    end
end

function get_run_history()
    runner = get_runner()
    lock(runner.lock) do
        copy(runner.run_history)
    end
end

function get_active_runs()
    runner = get_runner()
    lock(runner.lock) do
        filter(r -> r.status == :running, runner.run_history)
    end
end

function cancel_run(id::String)
    runner = get_runner()
    lock(runner.lock) do
        idx = findfirst(r -> r.id == id || startswith(r.id, id), runner.run_history)
        if idx !== nothing
            rec = runner.run_history[idx]
            if rec.status == :running && rec.cts !== nothing
                cancel(rec.cts)
                return true
            end
        end
        return false
    end
end

function get_last_run_id()
    runner = get_runner()
    lock(runner.lock) do
        isempty(runner.run_history) ? nothing : runner.run_history[1].id
    end
end

function get_run_result(id::String)
    runner = get_runner()
    cached = lock(runner.lock) do
        idx = findfirst(r -> r.id == id, runner.run_history)
        idx === nothing && return nothing
        runner.run_history[idx].result
    end
    cached !== nothing && return cached
    # If still running, build a snapshot from the live RunContext
    ctx = lock(runner.lock) do
        get(runner.run_contexts, id, nothing)
    end
    ctx === nothing && return nothing
    _build_result_from_context(runner, id, ctx)
end

function _convert_stack_frames(stack_trace)
    stack_trace === nothing && return nothing
    return TestrunResultStackFrame[
        TestrunResultStackFrame(
            frame.label,
            frame.uri === nothing ? URI("") : URI(frame.uri),
            something(frame.line, 0),
            something(frame.column, 0),
        ) for frame in stack_trace
    ]
end

function _convert_messages(messages)
    messages === nothing && return nothing
    return TestrunResultMessage[
        TestrunResultMessage(
            msg.message,
            msg.expected_output,
            msg.actual_output,
            msg.uri === nothing ? URI("") : URI(msg.uri),
            something(msg.line, 0),
            something(msg.column, 0),
            _convert_stack_frames(msg.stack_trace),
        ) for msg in messages
    ]
end

function _build_result_from_context(runner::TestItemRunner, testrun_id::String, ctx::RunContext)
    testitem_outputs = ctx.outputs
    collected_process_outputs = lock(runner.lock) do
        Dict{String,String}(pid => join(chunks) for (pid, chunks) in runner.process_outputs)
    end
    testitems = TestrunResultTestitem[
        TestrunResultTestitem(
            ti.testitem.label,
            URI(ti.testitem.uri),
            [TestrunResultTestitemProfile(
                ti.testenvironment.name,
                ti.result.status,
                ti.result.duration,
                _convert_messages(ti.result.messages),
                haskey(testitem_outputs, ti.testitem.id) ? join(testitem_outputs[ti.testitem.id]) : nothing
            )]
        ) for ti in ctx.responses
    ]
    TestrunResult(TestrunResultDefinitionError[], testitems, collected_process_outputs)
end

function _prune_history!(runner::TestItemRunner)
    # Keep at most _MAX_HISTORY entries, prune oldest completed
    while length(runner.run_history) > runner.max_history
        idx = findlast(r -> r.status != :running, runner.run_history)
        idx === nothing && break
        deleteat!(runner.run_history, idx)
    end
end

function terminate_process(id::String)
    if isassigned(_g_runner)
        TestItemControllers.terminate_test_process(_g_runner[].controller, id)
    end
end

# ── Runner initialization ─────────────────────────────────────────────

function get_runner()
    isassigned(_g_runner) && return _g_runner[]
    lock(_g_runner_lock) do
        isassigned(_g_runner) && return _g_runner[]

        callbacks = TestItemControllers.ControllerCallbacks(
            on_testitem_started = (testrun_id, testitem_id, test_env_id) -> nothing,
            on_testitem_passed = (testrun_id, testitem_id, test_env_id, duration) -> begin
                ctx = get_run_context(testrun_id)
                ctx === nothing && return
                ctx.count_success += 1
                testitem = ctx.testitems_by_id[testitem_id]
                if ctx.progress_ui == :log
                    duration_string = duration !== nothing ? " ($(duration)ms)" : ""
                    println("✓ $(ctx.environment_name) $(uri2filepath(URI(testitem.uri))):$(testitem.label) → passed$duration_string")
                end
                if ctx.progress_ui == :bar
                    ctx.progressbar_next()
                end
                push!(ctx.responses, (testitem=testitem, testenvironment=ctx.environments[1], result=(status=:passed, messages=nothing, duration=duration)))
            end,
            on_testitem_failed = (testrun_id, testitem_id, test_env_id, messages, duration) -> begin
                ctx = get_run_context(testrun_id)
                ctx === nothing && return
                ctx.count_fail += 1
                testitem = ctx.testitems_by_id[testitem_id]
                if ctx.progress_ui == :log
                    duration_string = duration !== nothing ? " ($(duration)ms)" : ""
                    println("✗ $(ctx.environment_name) $(uri2filepath(URI(testitem.uri))):$(testitem.label) → failed$duration_string")
                end
                if ctx.progress_ui == :bar
                    ctx.progressbar_next()
                end
                push!(ctx.responses, (testitem=testitem, testenvironment=ctx.environments[1], result=(status=:failed, messages=messages, duration=duration)))
            end,
            on_testitem_errored = (testrun_id, testitem_id, test_env_id, messages, duration) -> begin
                ctx = get_run_context(testrun_id)
                ctx === nothing && return
                ctx.count_error += 1
                testitem = ctx.testitems_by_id[testitem_id]
                if ctx.progress_ui == :log
                    duration_string = duration !== nothing ? " ($(duration)ms)" : ""
                    println("✗ $(ctx.environment_name) $(uri2filepath(URI(testitem.uri))):$(testitem.label) → errored$duration_string")
                end
                if ctx.progress_ui == :bar
                    ctx.progressbar_next()
                end
                push!(ctx.responses, (testitem=testitem, testenvironment=ctx.environments[1], result=(status=:errored, messages=messages, duration=duration)))
            end,
            on_testitem_skipped = (testrun_id, testitem_id, test_env_id) -> begin
                ctx = get_run_context(testrun_id)
                ctx === nothing && return
                ctx.count_skipped += 1
                testitem = ctx.testitems_by_id[testitem_id]
                if ctx.progress_ui == :log
                    println("⊘ $(ctx.environment_name) $(uri2filepath(URI(testitem.uri))):$(testitem.label) → skipped")
                end
                if ctx.progress_ui == :bar
                    ctx.progressbar_next()
                end
                push!(ctx.responses, (testitem=testitem, testenvironment=ctx.environments[1], result=(status=:skipped, messages=nothing, duration=nothing)))
            end,
            on_append_output = (testrun_id, testitem_id, test_env_id, output) -> begin
                ctx = get_run_context(testrun_id)
                ctx === nothing && return
                testitem_id === nothing && return  # process-level output; captured by on_process_output
                if !haskey(ctx.outputs, testitem_id)
                    ctx.outputs[testitem_id] = String[]
                end
                push!(ctx.outputs[testitem_id], output)
            end,
            on_attach_debugger = (testrun_id, debug_pipename) -> nothing,
            on_process_created = (id, test_env_id) -> begin
                runner = _g_runner[]
                env = lock(runner.lock) do
                    get(runner.test_env_by_id, test_env_id, nothing)
                end
                package_name = env !== nothing ? env.package_name : ""
                lock(runner.lock) do
                    runner.processes[id] = ProcessInfo(id, package_name, "Launching")
                end
            end,
            on_process_terminated = (id) -> begin
                runner = _g_runner[]
                lock(runner.lock) do
                    delete!(runner.processes, id)
                end
            end,
            on_process_status_changed = (id, status) -> begin
                runner = _g_runner[]
                lock(runner.lock) do
                    if haskey(runner.processes, id)
                        old = runner.processes[id]
                        runner.processes[id] = ProcessInfo(old.id, old.package_name, status)
                    end
                end
                # Print dot for each process launch event
                if status == "Launching"
                    lock(runner.lock) do
                        for ctx in values(runner.run_contexts)
                            if ctx.progress_ui == :bar
                                if !ctx.launch_header_printed
                                    ctx.launch_header_printed = true
                                    printstyled("  Launching test processes"; color=:cyan)
                                end
                                printstyled("."; color=:cyan)
                            end
                        end
                    end
                end
            end,
            on_process_output = (id, output) -> begin
                runner = _g_runner[]
                lock(runner.lock) do
                    if !haskey(runner.process_outputs, id)
                        runner.process_outputs[id] = String[]
                    end
                    push!(runner.process_outputs[id], output)
                end
            end,
        )

        controller = TestItemController(callbacks)
        runner = TestItemRunner(controller)
        _g_runner[] = runner
        @async try
            run(runner.controller)
        catch err
            Base.display_error(err, catch_backtrace())
        end

        return runner
    end
end

# ── Main entry point ──────────────────────────────────────────────────

function run_tests(
            path;
            filter=nothing,
            verbose=false,
            max_workers::Int=min(Sys.CPU_THREADS, 8),
            timeout=60*5,
            fail_on_detection_error=true,
            return_results=false,
            print_failed_results=true,
            print_summary=true,
            progress_ui=:bar,
            environments=[RunProfile("Default", false, Dict{String,Any}())],
            token=nothing
        )
    # Silent mode: suppress all terminal output
    if progress_ui == :none
        print_summary = false
        print_failed_results = false
    end

    runner = get_runner()
    tic = runner.controller
    runner.run_counter[] += 1
    testrun_id = string(runner.run_counter[])

    # Register in run history
    cts_for_history = if token !== nothing
        # Find or create a CTS for cancellation — token is read-only, so we wrap
        nothing
    else
        nothing
    end
    record = TestrunRecord(testrun_id, time(), nothing, :running, nothing, string(path), cts_for_history)
    lock(runner.lock) do
        pushfirst!(runner.run_history, record)
        _prune_history!(runner)
    end

    jw = JuliaWorkspaces.workspace_from_folders(([path]))
    
    # Flat list of @testitems and @testmodule and @testsnippet
    testitems = []
    testerrors = []
    for (uri, items) in pairs(JuliaWorkspaces.get_test_items(jw))
        project_details = JuliaWorkspaces.get_test_env(jw, uri)
        textfile = JuliaWorkspaces.get_text_file(jw, uri)

        for item in items.testitems            
            line, column = JuliaWorkspaces.position_at(textfile.content, item.code_range.start)
            push!(testitems, (
                uri=uri,
                line=line,
                column=column,
                code=textfile.content.content[item.code_range],
                env=project_details,
                detail=item),
            )
        end

        for item in items.testerrors
            line, column = JuliaWorkspaces.position_at(textfile.content, item.range.start)
            push!(testerrors,
                (
                    uri=string(uri),
                    line=line,
                    column=column,
                    message=item.message
                )
            )
        end
    end

    responses = []

    if length(testerrors) == 0  || fail_on_detection_error==false
        # Filter @testitems
        if filter !== nothing
            cd(path) do
                filter!(i->filter((filename=uri2filepath(i.uri), name=i.detail.name, tags=i.detail.option_tags, package_name=i.env.package_name)), testitems)
            end
        end

        n_total = length(testitems)*length(environments)

        if progress_ui != :none
            n_files = length(unique(i.uri for i in testitems))
            printstyled("  Discovered $n_total test item(s) in $n_files file(s)\n"; color=:cyan)
        end

        p = ProgressMeter.Progress(n_total;
            barglyphs=ProgressMeter.BarGlyphs('┣','━','╸',' ','┫'),
            color=:green, enabled=progress_ui==:bar)

        debuglogger = Logging.ConsoleLogger(stderr, Logging.Warn)

        environment_name = environments[1].name

        Logging.with_logger(debuglogger) do

            testitems_to_run_by_id = Dict{String, TestItemControllers.TestItemDetail}()
                    # Collect package info per item for building per-package TestEnvironments
                    item_package_info = Dict{String, NamedTuple{(:package_name, :package_uri, :project_uri, :env_content_hash), Tuple{String, String, Union{Nothing,String}, Union{Nothing,String}}}}()
                    for (uri, file_info) in pairs(JuliaWorkspaces.get_test_items(jw))
                        project_details = JuliaWorkspaces.get_test_env(jw, uri)
                        textfile = JuliaWorkspaces.get_text_file(jw, uri)
                        for item in file_info.testitems
                            testitems_to_run_by_id[item.id] = TestItemControllers.TestItemDetail(
                                item.id,
                                string(item.uri),
                                item.name,
                                project_details.package_name,
                                string(project_details.package_uri),
                                item.option_default_imports,
                                string.(item.option_setup),
                                JuliaWorkspaces.position_at(textfile.content, item.code_range.start)[1],
                                JuliaWorkspaces.position_at(textfile.content, item.code_range.start)[2],
                                textfile.content.content[item.code_range],
                                JuliaWorkspaces.position_at(textfile.content, item.code_range.stop)[1],
                                JuliaWorkspaces.position_at(textfile.content, item.code_range.stop)[2],
                            )
                            item_package_info[item.id] = (
                                package_name = project_details.package_name,
                                package_uri = string(project_details.package_uri),
                                project_uri = project_details.project_uri === nothing ? nothing : string(project_details.project_uri),
                                env_content_hash = project_details.env_content_hash === nothing ? nothing : string(project_details.env_content_hash),
                            )
                        end
                    end

            # Apply filter to the test items dict (the earlier filter on 'testitems' was only for counting)
            if filter !== nothing
                filtered_ids = Set(i.detail.id for i in testitems)
                for id in collect(keys(testitems_to_run_by_id))
                    if !(id in filtered_ids)
                        delete!(testitems_to_run_by_id, id)
                    end
                end
            end

            if isempty(testitems_to_run_by_id)
                @warn "No test items to run" filter_applied=(filter !== nothing)
            end

            # Register per-run context so callbacks can dispatch by testrun_id
            ctx = RunContext(
                testitems_to_run_by_id,
                environments,
                environment_name,
                progress_ui,
                () -> nothing,  # placeholder, replaced below
                0, 0, 0, 0,
                n_total,
                responses,
                Dict{String,Vector{String}}(),
                false,
            )

            # Define progressbar_next reading from ctx so counters update live
            ctx.progressbar_next = () -> begin
                if ctx.launch_header_printed
                    ctx.launch_header_printed = false
                    println()
                end
                done = ctx.count_success + ctx.count_fail + ctx.count_error + ctx.count_skipped
                parts = String[]
                ctx.count_success > 0 && push!(parts, "$(ctx.count_success) passed")
                ctx.count_fail > 0 && push!(parts, "$(ctx.count_fail) failed")
                ctx.count_error > 0 && push!(parts, "$(ctx.count_error) errored")
                ctx.count_skipped > 0 && push!(parts, "$(ctx.count_skipped) skipped")
                detail = isempty(parts) ? "" : " ($(join(parts, ", ")))"
                ProgressMeter.next!(
                    p,
                    showvalues = [
                        (Symbol("Progress"), "$done/$(ctx.n_total)$detail"),
                    ]
                )
            end

            lock(runner.lock) do
                runner.run_contexts[testrun_id] = ctx
            end

            ret = try
                # Collect unique packages from the discovered test items
                unique_packages = Dict{String, NamedTuple}()
                for (item_id, pkg) in item_package_info
                    haskey(testitems_to_run_by_id, item_id) || continue
                    key = pkg.package_uri
                    if !haskey(unique_packages, key)
                        unique_packages[key] = pkg
                    end
                end

                # Create one TestEnvironment per (package × profile) combination
                test_envs = TestItemControllers.TestEnvironment[]
                env_id_for_item = Dict{String, String}()  # item_id → env_id
                for profile in environments
                    env_vars = Dict{String,Union{String,Nothing}}(k => v isa AbstractString ? string(v) : v === nothing ? nothing : string(v) for (k,v) in profile.env)
                    mode = profile.coverage ? "Coverage" : "Normal"
                    for (pkg_uri, pkg) in unique_packages
                        env = TestItemControllers.TestEnvironment(
                            string(UUIDs.uuid4()),
                            "julia",
                            String[],
                            nothing,
                            env_vars,
                            mode,
                            pkg.package_name,
                            pkg.package_uri,
                            pkg.project_uri,
                            pkg.env_content_hash,
                        )
                        push!(test_envs, env)
                        for (item_id, item_pkg) in item_package_info
                            if item_pkg.package_uri == pkg_uri && haskey(testitems_to_run_by_id, item_id)
                                env_id_for_item[item_id] = env.id
                            end
                        end
                    end
                end

                # Register test environments for on_process_created callback
                lock(runner.lock) do
                    for env in test_envs
                        runner.test_env_by_id[env.id] = env
                    end
                end

                test_items = collect(values(testitems_to_run_by_id))
                work_units = [
                    TestItemControllers.TestRunItem(item.id, env_id_for_item[item.id], nothing, :Info)
                    for item in test_items
                ]
                test_setups = pairs(JuliaWorkspaces.get_test_items(jw)) |>
                    @map({uri = _.first, items = _.second.testsetups}) |>
                    @mutate(
                        project_details = JuliaWorkspaces.get_test_env(jw, _.uri),
                        textfile = JuliaWorkspaces.get_text_file(jw, _.uri)
                    ) |>
                    @filter(_.project_details.package_uri !== nothing) |>
                    @mapmany(
                        _.items,
                        TestItemControllers.TestSetupDetail(
                            string(_.project_details.package_uri),
                            string(__.name),
                            string(__.kind),
                            string(_.uri),
                            JuliaWorkspaces.position_at(_.textfile.content, __.code_range.start)[1],
                            JuliaWorkspaces.position_at(_.textfile.content, __.code_range.start)[2],
                            _.textfile.content.content[__.code_range]
                        )
                    ) |>
                    i-> collect(TestItemControllers.TestSetupDetail, i)
                TestItemControllers.execute_testrun(
                    tic,
                    testrun_id,
                    test_envs,
                    test_items,
                    work_units,
                    test_setups,
                    max_workers,
                    token,
                )
            catch err
                @error "TestItemControllers.execute_testrun failed" exception=(err, catch_backtrace())
                rethrow(err)
            finally
                # Snapshot partial results into history before deleting context;
                # on the success path this is overwritten by the full result below.
                try
                    partial = _build_result_from_context(runner, testrun_id, ctx)
                    lock(runner.lock) do
                        idx = findfirst(r -> r.id == testrun_id, runner.run_history)
                        if idx !== nothing && runner.run_history[idx].result === nothing
                            runner.run_history[idx].result = partial
                        end
                    end
                catch
                    # Best-effort — don't mask the original exception
                end
                lock(runner.lock) do
                    delete!(runner.run_contexts, testrun_id)
                end
            end

            # Extract coverage data if coverage mode is enabled
            if any(env -> env.coverage, environments) && ret !== nothing
                @info "Coverage data collected but not yet processed"
            end

            # Capture values from ctx before leaving this scope
            count_success = ctx.count_success
            count_fail = ctx.count_fail
            count_error = ctx.count_error
            count_skipped = ctx.count_skipped
            testitem_outputs = ctx.outputs

            # Print newline after launch dots (if any were printed)
            if ctx.launch_header_printed
                println()
            end
        end
    else
        count_success = 0
        count_fail = 0
        count_error = 0
        count_skipped = 0
        testitem_outputs = Dict{String,Vector{String}}()
    end

    if print_summary
        println()
        parts = String[]
        length(testerrors) > 0 && push!(parts, "$(length(testerrors)) definition error$(ifelse(length(testerrors)==1,"", "s"))")
        push!(parts, "$(length(responses)) tests ran")
        count_success > 0 && push!(parts, "\e[32m$(count_success) passed\e[0m")
        count_fail > 0 && push!(parts, "\e[31m$(count_fail) failed\e[0m")
        count_error > 0 && push!(parts, "\e[31m$(count_error) errored\e[0m")
        count_skipped > 0 && push!(parts, "$(count_skipped) skipped")
        println(join(parts, ", "), ".")
    end

    if print_failed_results
        for te in testerrors
            println()
            println("Definition error at $(uri2filepath(URI(te.uri))):$(te.line)")
            println("  $(te.message)")
        end
    
        for i in responses
            if i.result.status in (:failed, :errored) 
                println()
                label = i.result.status == :failed ? "FAIL" : "ERROR"
                printstyled("  [$label] $(i.testitem.label)"; color=:red, bold=true)
                if i.result.duration !== nothing
                    print(" ($(i.result.duration)ms)")
                end
                println()
                if i.result.messages!==nothing                
                    for j in i.result.messages
                        println("    ", replace(j.message, "\n"=>"\n    "))
                        if j.expected_output !== nothing || j.actual_output !== nothing
                            j.expected_output !== nothing && println("    Expected: ", replace(j.expected_output, "\n"=>"\n             "))
                            j.actual_output !== nothing && println("    Actual:   ", replace(j.actual_output, "\n"=>"\n             "))
                        end
                        if j.stack_trace !== nothing
                            for frame in j.stack_trace
                                frame_uri = frame.uri !== nothing ? frame.uri : "?"
                                frame_line = something(frame.line, 0)
                                frame_col = something(frame.column, 0)
                                println("      at $(frame.label) ($(frame_uri):$(frame_line):$(frame_col))")
                            end
                        end
                    end
                end
            end
        end
    end

    # Collect process outputs snapshot
    collected_process_outputs = lock(runner.lock) do
        Dict{String,String}(id => join(chunks) for (id, chunks) in runner.process_outputs)
    end

    duplicated_testitems = TestrunResultTestitem[
        TestrunResultTestitem(
            ti.testitem.label,
            URI(ti.testitem.uri),
            [TestrunResultTestitemProfile(
                ti.testenvironment.name,
                ti.result.status,
                ti.result.duration,
                _convert_messages(ti.result.messages),
                haskey(testitem_outputs, ti.testitem.id) ? join(testitem_outputs[ti.testitem.id]) : nothing
            )]
        ) for ti in responses
    ]

    deduplicated_testitems = duplicated_testitems |>
        @groupby({_.name, _.uri}) |>
        @map(TestrunResultTestitem(key(_).name, key(_).uri, [_.profiles...;])) |>
        collect

    typed_results = TestrunResult(
        TestrunResultDefinitionError[TestrunResultDefinitionError(i.message, URI(i.uri), i.line, i.column) for i in testerrors],
        deduplicated_testitems,
        collected_process_outputs,
    )

    # Update run history with result
    lock(runner.lock) do
        idx = findfirst(r -> r.id == testrun_id, runner.run_history)
        if idx !== nothing
            runner.run_history[idx].result = typed_results
            runner.run_history[idx].status = :completed
            runner.run_history[idx].end_time = time()
        end
    end

    return return_results ? typed_results : testrun_id
end

function kill_test_processes()
    if isassigned(_g_runner)
        TestItemControllers.shutdown(_g_runner[].controller)
    end
end

end
