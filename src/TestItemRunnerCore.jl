module TestItemRunnerCore

export run_tests, kill_test_processes, terminate_process, get_active_processes,
       RunProfile, ProcessInfo,
       TestrunResult, TestrunResultTestitem, TestrunResultTestitemProfile,
       TestrunResultMessage, TestrunResultDefinitionError

# Re-export CancellationTokens API so consumers don't need to reach into internals
export CancellationTokenSource, CancellationToken, cancel, get_token, is_cancellation_requested

import ProgressMeter, JuliaWorkspaces, AutoHashEquals, TestItemControllers, Logging
using UUIDs
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

struct TestrunResultMessage
    message::String
    uri::URI
    line::Int
    column::Int
end

struct TestrunResultTestitemProfile
    profile_name::String
    status::Symbol
    duration::Union{Float64,Missing}
    messages::Union{Vector{TestrunResultMessage},Missing}
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
    responses::Vector{Any}
end

const g_run_contexts = Dict{String,RunContext}()
const g_run_contexts_lock = ReentrantLock()

function get_run_context(testrun_id::String)
    lock(g_run_contexts_lock) do
        get(g_run_contexts, testrun_id, nothing)
    end
end

# ── Process tracking ──────────────────────────────────────────────────

const g_processes = Dict{String,ProcessInfo}()
const g_processes_lock = ReentrantLock()

function get_active_processes()
    lock(g_processes_lock) do
        collect(values(g_processes))
    end
end

function terminate_process(id::String)
    if isassigned(g_testitemcontroller)
        TestItemControllers.terminate_test_process(g_testitemcontroller[], id)
    end
end

# ── Controller singleton ─────────────────────────────────────────────

const g_testitemcontroller = Ref{TestItemController}()

function get_testitemcontroller()
    if !isassigned(g_testitemcontroller)
        callbacks = ControllerCallbacks(
            on_testitem_started = (testrun_id, testitem_id) -> nothing,
            on_testitem_passed = (testrun_id, testitem_id, duration) -> begin
                ctx = get_run_context(testrun_id)
                ctx === nothing && return
                ctx.count_success += 1
                testitem = ctx.testitems_by_id[testitem_id]
                if ctx.progress_ui == :log
                    duration_string = duration !== missing ? " ($(duration)ms)" : ""
                    println("✓ $(ctx.environment_name) $(uri2filepath(URI(testitem.uri))):$(testitem.label) → passed$duration_string")
                end
                if ctx.progress_ui == :bar
                    ctx.progressbar_next()
                end
                push!(ctx.responses, (testitem=testitem, testenvironment=ctx.environments[1], result=(status=:passed, messages=missing, duration=duration)))
            end,
            on_testitem_failed = (testrun_id, testitem_id, messages, duration) -> begin
                ctx = get_run_context(testrun_id)
                ctx === nothing && return
                ctx.count_fail += 1
                testitem = ctx.testitems_by_id[testitem_id]
                if ctx.progress_ui == :log
                    duration_string = duration !== missing ? " ($(duration)ms)" : ""
                    println("✗ $(ctx.environment_name) $(uri2filepath(URI(testitem.uri))):$(testitem.label) → failed$duration_string")
                end
                if ctx.progress_ui == :bar
                    ctx.progressbar_next()
                end
                push!(ctx.responses, (testitem=testitem, testenvironment=ctx.environments[1], result=(status=:failed, messages=messages, duration=duration)))
            end,
            on_testitem_errored = (testrun_id, testitem_id, messages, duration) -> begin
                ctx = get_run_context(testrun_id)
                ctx === nothing && return
                ctx.count_error += 1
                testitem = ctx.testitems_by_id[testitem_id]
                if ctx.progress_ui == :log
                    duration_string = duration !== missing ? " ($(duration)ms)" : ""
                    println("✗ $(ctx.environment_name) $(uri2filepath(URI(testitem.uri))):$(testitem.label) → errored$duration_string")
                end
                if ctx.progress_ui == :bar
                    ctx.progressbar_next()
                end
                push!(ctx.responses, (testitem=testitem, testenvironment=ctx.environments[1], result=(status=:errored, messages=messages, duration=duration)))
            end,
            on_testitem_skipped = (testrun_id, testitem_id) -> begin
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
                push!(ctx.responses, (testitem=testitem, testenvironment=ctx.environments[1], result=(status=:skipped, messages=missing, duration=missing)))
            end,
            on_append_output = (testrun_id, testitem_id, output) -> nothing,
            on_attach_debugger = (testrun_id, debug_pipename) -> nothing,
            on_process_created = (id, package_name, package_uri, project_uri, coverage, env) -> begin
                lock(g_processes_lock) do
                    g_processes[id] = ProcessInfo(id, package_name, "Launching")
                end
            end,
            on_process_terminated = (id) -> begin
                lock(g_processes_lock) do
                    delete!(g_processes, id)
                end
            end,
            on_process_status_changed = (id, status) -> begin
                lock(g_processes_lock) do
                    if haskey(g_processes, id)
                        old = g_processes[id]
                        g_processes[id] = ProcessInfo(old.id, old.package_name, status)
                    end
                end
            end,
            on_process_output = (id, output) -> nothing,
        )

        g_testitemcontroller[] = TestItemController(callbacks)
        @async try
            run(g_testitemcontroller[])
        catch err
            Base.display_error(err, catch_backtrace())
        end
    end

    return g_testitemcontroller[]
end

# ── Main entry point ──────────────────────────────────────────────────

function run_tests(
            path;
            filter=nothing,
            verbose=false,
            max_workers::Int=Sys.CPU_THREADS,
            timeout=60*5,
            fail_on_detection_error=true,
            return_results=false,
            print_failed_results=true,
            print_summary=true,
            progress_ui=:bar,
            environments=[RunProfile("Default", false, Dict{String,Any}())],
            token=nothing
        )
    tic = get_testitemcontroller()
    testrun_id = string(uuid4())

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

    count_success = 0
    count_timeout = 0
    count_fail = 0
    count_error = 0
    count_crash = 0
    count_skipped = 0

    p = ProgressMeter.Progress(0, barlen=50, enabled=false)

    progressbar_next = () -> begin
        ProgressMeter.next!(
            p,
            showvalues = [
                (Symbol("Successful tests"), count_success),
                (Symbol("Failed tests"), count_fail),
                (Symbol("Errored tests"), count_error),
                (Symbol("Crashed tests"), count_crash),
                (Symbol("Timed out tests"), count_timeout),
                (Symbol("Skipped tests"), count_skipped),
            ]
        )
    end

    responses = []

    if length(testerrors) == 0  || fail_on_detection_error==false
        # Filter @testitems
        if filter !== nothing
            cd(path) do
                filter!(i->filter((filename=uri2filepath(i.uri), name=i.detail.name, tags=i.detail.option_tags, package_name=i.env.package_name)), testitems)
            end
        end

        p = ProgressMeter.Progress(length(testitems)*length(environments), barlen=50, enabled=progress_ui==:bar)

        debuglogger = Logging.ConsoleLogger(stderr, Logging.Warn)

        environment_name = environments[1].name

        Logging.with_logger(debuglogger) do

            testitems_to_run_by_id = pairs(JuliaWorkspaces.get_test_items(jw)) |>
                    @map({uri = _.first, items = _.second.testitems}) |>
                    @mutate(
                        project_details = JuliaWorkspaces.get_test_env(jw, _.uri),
                        textfile = JuliaWorkspaces.get_text_file(jw, _.uri)
                    ) |>
                    @mapmany(
                        _.items,
                        __.id => 
                        TestItemControllers.TestItemDetail(
                            __.id,
                            string(__.uri),
                            __.name,
                            _.project_details.package_name,
                            string(_.project_details.package_uri),
                            _.project_details.project_uri === nothing ? nothing : string(_.project_details.project_uri),
                            string(_.project_details.env_content_hash),
                            __.option_default_imports,
                            string.(__.option_setup),
                            JuliaWorkspaces.position_at(_.textfile.content, __.code_range.start)[1],
                            JuliaWorkspaces.position_at(_.textfile.content, __.code_range.start)[2],
                            _.textfile.content.content[__.code_range],
                            JuliaWorkspaces.position_at(_.textfile.content, __.code_range.stop)[1],
                            JuliaWorkspaces.position_at(_.textfile.content, __.code_range.stop)[2],
                            nothing # timeout
                        )
                    ) |>
                    Dict

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
                progressbar_next,
                0, 0, 0, 0,
                responses,
            )
            lock(g_run_contexts_lock) do
                g_run_contexts[testrun_id] = ctx
            end

            ret = try
                TestItemControllers.execute_testrun(
                    tic,
                    testrun_id,
                    [
                        TestItemControllers.TestProfile(
                            i.name,
                            "$(i.name) Profile",
                            "julia",
                            String[],
                            missing,
                            Dict{String,Union{String,Nothing}}(k => v isa AbstractString ? string(v) : v === nothing ? nothing : string(v) for (k,v) in i.env),
                            max_workers,
                            i.coverage ? "Coverage" : "Normal",
                            nothing,
                            :Info
                        ) for i in environments
                    ],
                    collect(values(testitems_to_run_by_id)),
                    pairs(JuliaWorkspaces.get_test_items(jw)) |>
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
                        i-> collect(TestItemControllers.TestSetupDetail, i),
                    token,
                )
            catch err
                @error "TestItemControllers.execute_testrun failed" exception=(err, catch_backtrace())
                rethrow(err)
            finally
                # Sync counts back from context
                count_success = ctx.count_success
                count_fail = ctx.count_fail
                count_error = ctx.count_error
                count_skipped = ctx.count_skipped

                lock(g_run_contexts_lock) do
                    delete!(g_run_contexts, testrun_id)
                end
            end

            # Extract coverage data if coverage mode is enabled
            if any(env -> env.coverage, environments) && ret !== missing && ret !== nothing
                @info "Coverage data collected but not yet processed"
            end
        end
    end

    if print_summary
        summaries = String[]

        if length(testerrors)>0
            push!(summaries, "$(length(testerrors)) definition error$(ifelse(length(testerrors)==1,"", "s"))")
        end

        push!(summaries, "$(length(responses)) tests ran")
        push!(summaries, "$(count_success) passed")
        push!(summaries, "$(count_fail) failed")
        push!(summaries, "$(count_error) errored")
        push!(summaries, "$(count_skipped) skipped")
        push!(summaries, "$(count_crash) crashed")
        push!(summaries, "$(count_timeout) timed out")

        println()
        println(join(summaries, ", "), ".")
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

                if i.result.status == :failed
                    println("Test failure in $(uri2filepath(URI(i.testitem.uri))):$(i.testitem.label)")
                elseif i.result.status == :errored
                    println("Test error in $(uri2filepath(URI(i.testitem.uri))):$(i.testitem.label)")
                end

                if i.result.messages!==missing                
                    for j in i.result.messages
                        println("  at $(uri2filepath(URI(j.uri))):$(j.line)")
                        println("    ", replace(j.message, "\n"=>"\n    "))
                    end
                end
            end
        end
    end

    if return_results
        duplicated_testitems = TestrunResultTestitem[TestrunResultTestitem(ti.testitem.label, URI(ti.testitem.uri), [TestrunResultTestitemProfile(ti.testenvironment.name, ti.result.status, ti.result.duration, ti.result.messages===missing ? missing : [TestrunResultMessage(msg.message, msg.uri === missing ? URI("") : URI(msg.uri), coalesce(msg.line, 0), coalesce(msg.column, 0)) for msg in ti.result.messages])]) for ti in responses]

        deduplicated_testitems = duplicated_testitems |>
            @groupby({_.name, _.uri}) |>
            @map(TestrunResultTestitem(key(_).name, key(_).uri, [_.profiles...;])) |>
            collect

        typed_results = TestrunResult(
            TestrunResultDefinitionError[TestrunResultDefinitionError(i.message, URI(i.uri), i.line, i.column) for i in testerrors],
            deduplicated_testitems
        )
        return typed_results
    else
        return nothing
    end
end

function kill_test_processes()
    if isassigned(g_testitemcontroller)
        TestItemControllers.shutdown(g_testitemcontroller[])
    end
end

end
