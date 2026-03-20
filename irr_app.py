import streamlit as st

from postgres_sync import DatabaseSyncError
from workflow_db import (
    close_inspector_session,
    complete_inspection_attempt,
    create_inspection_attempt,
    create_inspector_session,
    determine_shift,
    evaluate_measurements,
    find_recipe_candidates,
    get_attempt_measurements,
    get_connection_types,
    get_cnc_operators,
    get_employee_by_adp,
    get_locations,
    get_manager_candidates,
    get_ncr_reports,
    get_open_work_orders,
    get_pipe_attempt_history,
    get_pipe_unit,
    get_recipe_elements,
    has_manager_pin,
    initialize_workflow_schema,
    is_admin_user,
    is_manager_or_supervisor,
    search_pipe_units,
    set_manager_pin,
    update_ncr_report,
)


st.set_page_config(page_title="AutoIRR", layout="wide")


def inject_custom_styles():
    """Apply a warmer, more polished visual theme to the Streamlit app."""
    st.markdown(
        """
        <style>
        .stApp {
            background:
                radial-gradient(circle at top left, rgba(11, 110, 79, 0.16), transparent 28%),
                radial-gradient(circle at top right, rgba(219, 139, 34, 0.18), transparent 24%),
                linear-gradient(180deg, #f4f1ea 0%, #efe8da 100%);
            color: #1d2a28;
        }

        .block-container {
            padding-top: 2rem;
            padding-bottom: 2.5rem;
            max-width: 1400px;
        }

        h1, h2, h3 {
            color: #16342d;
            letter-spacing: -0.02em;
        }

        [data-testid="stSidebar"] {
            background: linear-gradient(180deg, #14352d 0%, #22463d 100%);
        }

        [data-testid="stSidebar"] * {
            color: #f5f1e8;
        }

        div[data-testid="stForm"] {
            background: rgba(255, 252, 246, 0.92);
            border: 1px solid rgba(22, 52, 45, 0.09);
            border-radius: 18px;
            padding: 1.1rem 1rem 0.5rem 1rem;
            box-shadow: 0 18px 40px rgba(56, 52, 42, 0.08);
        }

        div[data-testid="stExpander"] {
            background: rgba(255, 252, 246, 0.82);
            border: 1px solid rgba(22, 52, 45, 0.1);
            border-radius: 16px;
            overflow: hidden;
        }

        button[kind="primary"],
        .stButton > button {
            border-radius: 999px;
            border: none;
            background: linear-gradient(135deg, #0e6b57 0%, #1d8a72 100%);
            color: #ffffff;
            font-weight: 600;
            text-shadow: 0 1px 1px rgba(8, 28, 23, 0.28);
            letter-spacing: 0.01em;
            box-shadow: 0 10px 24px rgba(14, 107, 87, 0.22);
        }

        button[kind="primary"]:hover,
        .stButton > button:hover {
            background: linear-gradient(135deg, #0d5e4d 0%, #17735f 100%);
            color: #ffffff;
        }

        div[data-baseweb="select"] > div,
        .stTextInput input,
        .stTextArea textarea {
            border-radius: 14px;
            background: rgba(255, 252, 246, 0.96);
        }

        .stTabs [data-baseweb="tab-list"] {
            gap: 0.5rem;
            background: rgba(255, 252, 246, 0.72);
            border-radius: 999px;
            padding: 0.35rem;
        }

        .stTabs [data-baseweb="tab"] {
            border-radius: 999px;
            padding: 0.55rem 1rem;
            color: #35524a;
        }

        .stTabs [aria-selected="true"] {
            background: linear-gradient(135deg, #16342d 0%, #235446 100%);
            color: #fff7eb;
        }

        .autoirr-hero {
            background: linear-gradient(135deg, rgba(20, 53, 45, 0.96), rgba(40, 89, 75, 0.9));
            color: #fff8ef;
            border-radius: 24px;
            padding: 1.4rem 1.5rem;
            box-shadow: 0 18px 42px rgba(28, 44, 39, 0.18);
            margin-bottom: 1rem;
        }

        .autoirr-hero p {
            margin: 0.35rem 0 0 0;
            color: rgba(255, 248, 239, 0.84);
        }

        .autoirr-badge-row {
            display: flex;
            flex-wrap: wrap;
            gap: 0.6rem;
            margin-top: 0.9rem;
        }

        .autoirr-badge {
            display: inline-flex;
            align-items: center;
            padding: 0.35rem 0.8rem;
            border-radius: 999px;
            background: rgba(255, 248, 239, 0.14);
            border: 1px solid rgba(255, 248, 239, 0.14);
            font-size: 0.92rem;
        }

        .autoirr-section {
            background: rgba(255, 252, 246, 0.9);
            border: 1px solid rgba(22, 52, 45, 0.09);
            border-radius: 20px;
            padding: 1rem 1.1rem;
            box-shadow: 0 14px 32px rgba(56, 52, 42, 0.07);
            margin-bottom: 1rem;
        }

        .autoirr-section h3 {
            margin: 0 0 0.25rem 0;
        }

        .autoirr-section p {
            margin: 0;
            color: #526963;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_hero_panel(title, subtitle, badges=None):
    badge_markup = "".join(
        f"<span class='autoirr-badge'>{badge}</span>" for badge in (badges or [])
    )
    st.markdown(
        f"""
        <div class="autoirr-hero">
            <h1>{title}</h1>
            <p>{subtitle}</p>
            <div class="autoirr-badge-row">{badge_markup}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_section_intro(title, subtitle):
    st.markdown(
        f"""
        <div class="autoirr-section">
            <h3>{title}</h3>
            <p>{subtitle}</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _clear_inspection_form_state():
    """Remove transient inspection form state so the next pipe starts fresh."""
    active_inspection = st.session_state.get("active_inspection")
    if active_inspection:
        attempt_id = active_inspection.get("attempt_id")
        inspection_plan = active_inspection.get("inspection_plan") or []
        for element in inspection_plan:
            sequence = element.get("element_sequence")
            st.session_state.pop(f"bool_{attempt_id}_{sequence}", None)
            st.session_state.pop(f"measured_{attempt_id}_{sequence}", None)

    st.session_state.active_inspection = None
    st.session_state.pop("inspection_pipe_number", None)


def _init_state():
    if "session_record" not in st.session_state:
        st.session_state.session_record = None
    if "inspector" not in st.session_state:
        st.session_state.inspector = None
    if "shift" not in st.session_state:
        st.session_state.shift = determine_shift()
    if "active_inspection" not in st.session_state:
        st.session_state.active_inspection = None
    if "pending_login_context" not in st.session_state:
        st.session_state.pending_login_context = None


def get_user_role_label(user):
    """Return a short display label for the current user's access level."""
    if is_manager_or_supervisor(user):
        return "Manager/Supervisor"
    if is_admin_user(user):
        return "Admin Access Only"
    return "Inspector"


def render_admin():
    inspector = st.session_state.inspector
    if not is_admin_user(inspector):
        st.error("Admin Tools are available only to managers, supervisors, and IT.")
        return

    render_hero_panel(
        "AutoIRR Admin",
        "Administrative tools for setup and maintenance.",
        badges=[
            f"User: {inspector['name']}",
            f"Role: {get_user_role_label(inspector)}",
            f"Branch: {inspector['branch'] or 'Unknown'}",
            f"Department: {inspector.get('department') or 'Unknown'}",
        ],
    )

    manager_candidates = get_manager_candidates(inspector["branch"])
    if not manager_candidates:
        st.warning("No manager or supervisor candidates were found for this branch.")
        return

    render_section_intro(
        "Manager PIN Setup",
        "Create or update approval PINs for managers and supervisors in the current branch.",
    )
    with st.form("manager_pin_setup_form"):
        manager_names = [item["name"] for item in manager_candidates]
        selected_manager_name = st.selectbox("Manager", manager_names)
        new_pin = st.text_input("New Manager PIN", type="password")
        confirm_pin = st.text_input("Confirm Manager PIN", type="password")
        save_pin = st.form_submit_button("Save Manager PIN")

    if save_pin:
        if not new_pin.strip():
            st.error("Enter a PIN before saving.")
        elif new_pin != confirm_pin:
            st.error("PIN values do not match.")
        else:
            selected_manager = next(
                item for item in manager_candidates if item["name"] == selected_manager_name
            )
            try:
                set_manager_pin(selected_manager, new_pin)
            except ValueError as error:
                st.error(str(error))
            else:
                st.success(f"Manager PIN saved for {selected_manager_name}.")


def render_login():
    render_hero_panel(
        "AutoIRR",
        "Inspection login, operator selection, work order setup, and recipe-driven inspection entry.",
        badges=["Modernized inspection workflow", "SharePoint synced", "PostgreSQL backed"],
    )

    try:
        initialize_workflow_schema()
    except DatabaseSyncError as error:
        st.error(f"Database setup failed: {error}")
        st.stop()
    except Exception as error:
        st.error(f"Unexpected database setup error: {error}")
        st.stop()

    with st.form("login_form"):
        adp_number = st.text_input("Inspector ADP Number")
        shift = st.selectbox("Shift", ["Day", "Night"], index=0 if determine_shift() == "Day" else 1)
        locations = get_locations()
        location_names = [item["location_name"] for item in locations]
        selected_location_name = st.selectbox("Location / Machine", location_names)
        submitted = st.form_submit_button("Find Inspector")

    if submitted:
        if not adp_number.strip():
            st.error("Enter an ADP number before continuing.")
            return

        inspector = get_employee_by_adp(adp_number)
        if not inspector:
            st.error("No employee found for that ADP number in the synced Employees list.")
            st.session_state.pending_login_context = None
            return

        selected_location = next(
            item for item in locations if item["location_name"] == selected_location_name
        )
        st.session_state.pending_login_context = {
            "inspector": inspector,
            "shift": shift,
            "location": selected_location,
        }

    pending_context = st.session_state.pending_login_context
    if not pending_context:
        return

    inspector = pending_context["inspector"]
    shift = pending_context["shift"]
    selected_location = pending_context["location"]
    operators = get_cnc_operators(inspector["branch"])

    st.success(f"Inspector found: {inspector['name']}")
    st.markdown(
        f"""
        <div class="autoirr-section">
            <h3>Inspector Ready</h3>
            <p>
                Role: {get_user_role_label(inspector)} |
                Branch: {inspector['branch'] or 'Unknown'} |
                Department: {inspector.get('department') or 'Unknown'} |
                Shift: {shift} |
                Location: {selected_location['location_name']}
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    if not operators:
        st.warning("No machinists were found for this branch.")
        return

    with st.form("operator_form"):
        operator_names = [item["name"] for item in operators]
        selected_operator_name = st.selectbox("CNC Operator", operator_names)
        operator_submitted = st.form_submit_button("Start Session")

    if not operator_submitted:
        return

    selected_operator = next(item for item in operators if item["name"] == selected_operator_name)
    st.session_state.inspector = inspector
    st.session_state.shift = shift
    st.session_state.active_inspection = None
    st.session_state.pending_login_context = None
    st.session_state.session_record = create_inspector_session(
        inspector=inspector,
        shift=shift,
        location=selected_location,
        cnc_operator=selected_operator,
    )
    st.rerun()


def render_inspection_tab(inspector, session_record):
    render_section_intro(
        "Inspection Entry",
        "Select the work order, confirm the recipe, and capture the next pipe inspection.",
    )
    work_orders = get_open_work_orders(inspector["branch"])
    production_numbers = sorted({item["production_number"] for item in work_orders})
    if not production_numbers:
        st.warning("No open EN work orders were found for this branch.")
        return

    selected_production = st.selectbox("Production Number / WO", production_numbers)
    connection_types = get_connection_types(selected_production, inspector["branch"])
    if not connection_types:
        st.warning("No operation descriptions were found for this work order.")
        return

    description_map = {
        item["operation_description"]: item for item in connection_types
    }
    selected_description = st.selectbox(
        "Connection Type / Operation Description",
        list(description_map.keys()),
    )

    st.subheader("Recipe Match")
    recipe_candidates = find_recipe_candidates(selected_description, inspector["branch"])
    if recipe_candidates:
        recipe_names = [item["recipe_name"] for item in recipe_candidates]
        selected_recipe_name = st.selectbox("Suggested Recipe", recipe_names)
        selected_recipe = next(
            item for item in recipe_candidates if item["recipe_name"] == selected_recipe_name
        )
        st.caption(f"Match type: {selected_recipe['match_type']}")
    else:
        selected_recipe_name = st.text_input("Recipe Name", placeholder="Enter recipe manually")
        st.info("No recipe alias or token match was found. Manual recipe selection is needed.")

    pipe_number = st.text_input("Pipe Number", key="inspection_pipe_number")
    recipe_definition = None
    if selected_recipe_name:
        recipe_definition = get_recipe_elements(selected_recipe_name, inspector["branch"])
        with st.expander("Recipe Elements Preview", expanded=False):
            if recipe_definition and recipe_definition["elements"]:
                if recipe_definition.get("drawing"):
                    st.caption(
                        f"Drawing: {recipe_definition['drawing']} | "
                        f"Version: {recipe_definition.get('recipe_version') or 'n/a'}"
                    )
                st.dataframe(
                    [
                        {
                            "Seq": item["element_sequence"],
                            "Element": item["element_description"],
                            "DWG DIM": item["dwg_dim"],
                            "Gauge": item["gauge"],
                            "Type": item.get("capture_type"),
                            "Freq": item.get("frequency"),
                        }
                        for item in recipe_definition["elements"]
                    ],
                    use_container_width=True,
                    hide_index=True,
                )
            else:
                st.warning("No recipe elements were found for this recipe in the synced data.")

    if pipe_number.strip():
        existing_pipe = get_pipe_unit(selected_production, selected_description, pipe_number)
        if existing_pipe:
            st.warning(
                f"Pipe {pipe_number.strip()} already exists for this WO/connection. "
                f"This inspection will be a re-work attempt #{existing_pipe['latest_attempt_no'] + 1}."
            )
            history = get_pipe_attempt_history(existing_pipe["id"])
            if history:
                st.dataframe(history, use_container_width=True, hide_index=True)
        else:
            st.info("This pipe number has not been inspected yet for this WO/connection.")

    can_prepare = bool(
        selected_recipe_name
        and pipe_number.strip()
        and recipe_definition
        and recipe_definition["elements"]
    )
    if st.button("Prepare Inspection", disabled=not can_prepare, use_container_width=True):
        st.session_state.active_inspection = create_inspection_attempt(
            production_number=selected_production,
            operation_description=selected_description,
            pipe_number=pipe_number.strip(),
            branch=inspector["branch"],
            session_id=session_record["id"],
            inspector=inspector,
            cnc_operator={
                "item_id": session_record.get("cnc_operator_item_id"),
                "name": session_record.get("cnc_operator_name"),
            },
            recipe_name=selected_recipe_name,
            recipe_elements=recipe_definition,
        )
        st.rerun()

    active_inspection = st.session_state.active_inspection
    if not active_inspection:
        return

    st.subheader("Current Attempt")
    st.markdown(
        f"""
        <div class="autoirr-section">
            <h3>Current Attempt</h3>
            <p>
                Pipe Attempt: #{active_inspection['attempt_no']} |
                {"Re-work" if active_inspection['is_rework'] else "First inspection"}
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    inspection_plan = active_inspection["inspection_plan"]
    if inspection_plan:
        st.caption("Only the due elements for this pipe are shown below.")
    else:
        st.warning("No inspection elements were scheduled for this attempt.")

    with st.form("inspection_form"):
        measurement_inputs = []
        for element in inspection_plan:
            st.markdown(
                f"**{element['element_sequence']}. {element['element_description']}**  \n"
                f"DWG DIM: `{element.get('dwg_dim') or ''}` | Gauge: `{element.get('gauge') or ''}`"
            )
            if element.get("capture_type") == "boolean":
                measured_value = st.selectbox(
                    f"Result - {element['element_sequence']}",
                    ["Yes", "No"],
                    key=f"bool_{active_inspection['attempt_id']}_{element['element_sequence']}",
                )
            else:
                measured_value = st.text_input(
                    f"Measured Value - {element['element_sequence']}",
                    key=f"measured_{active_inspection['attempt_id']}_{element['element_sequence']}",
                )
            measurement_inputs.append(
                {
                    "element_sequence": element["element_sequence"],
                    "element_description": element["element_description"],
                    "dwg_dim": element.get("dwg_dim"),
                    "gauge": element.get("gauge"),
                    "capture_type": element.get("capture_type"),
                    "value_format": element.get("value_format"),
                    "nominal": element.get("nominal"),
                    "min": element.get("min"),
                    "max": element.get("max"),
                    "measured_value": measured_value,
                    "inspected_this_pipe": True,
                }
            )

        evaluation = evaluate_measurements(
            measurement_inputs,
            approval_rules=active_inspection.get("approval_rules", []),
        )
        evaluated_measurements = evaluation["measurements"]
        any_failures = evaluation["has_failures"]
        requires_approval = evaluation["requires_approval"]
        disposition_options = ["pass"] if not any_failures else [
            "manager_approved",
            "rework",
            "scrapped",
        ]
        disposition = st.selectbox(
            "Inspection Outcome",
            disposition_options,
            format_func=lambda value: {
                "pass": "Pass",
                "manager_approved": "Pass with Manager Approval",
                "rework": "Send to Re-work",
                "scrapped": "Scrap",
            }[value],
        )

        manager_name = ""
        manager_item_id = None
        manager_pin = ""
        tier_code = ""
        nonconformance = ""
        immediate_containment = ""

        if any_failures:
            st.warning(
                "One or more measurements are out of spec. "
                + (
                    "Manager approval is required for this failure pattern."
                    if requires_approval
                    else ""
                )
            )
            st.markdown("**NCR Information**")
            tier_code = st.text_input("Tier Code")
            nonconformance = st.text_area("Nonconformance")
            immediate_containment = st.text_area("Immediate Containment")

        if disposition == "manager_approved":
            st.markdown("**Manager Approval**")
            manager_candidates = get_manager_candidates(inspector["branch"])
            manager_options = {item["name"]: item for item in manager_candidates}
            if manager_options:
                manager_name = st.selectbox("Manager", list(manager_options.keys()))
                manager_item_id = manager_options[manager_name]["item_id"]
                if not has_manager_pin(manager_item_id):
                    st.info(
                        "This manager does not have a PIN configured yet. Use Manager PIN Setup first."
                    )
            else:
                manager_name = st.text_input("Manager Name")
            manager_pin = st.text_input("Manager PIN", type="password")

        notes = st.text_area("Attempt Notes")
        submitted = st.form_submit_button("Complete Inspection")

    if submitted:
        if any_failures and requires_approval and disposition != "manager_approved":
            st.error("This recipe requires manager approval for the detected failure condition.")
            return
        if disposition == "manager_approved" and (
            not manager_name.strip() or not manager_pin.strip()
        ):
            st.error("Manager name and PIN are required for manager approval.")
            return

        try:
            result = complete_inspection_attempt(
                attempt_id=active_inspection["attempt_id"],
                pipe_unit_id=active_inspection["pipe_unit_id"],
                measurements=evaluated_measurements,
                disposition=disposition,
                notes=notes,
                manager_item_id=manager_item_id,
                manager_name=manager_name,
                manager_pin=manager_pin,
                ncr_data={
                    "tier_code": tier_code,
                    "nonconformance": nonconformance,
                    "immediate_containment": immediate_containment,
                },
            )
        except ValueError as error:
            st.error(str(error))
            return

        st.success(
            f"Inspection saved. Attempt status: {result['attempt_status']}. "
            f"Pipe status: {result['pipe_status']}."
        )
        _clear_inspection_form_state()
        st.rerun()


def render_pipe_history_tab(inspector):
    render_section_intro(
        "Pipe History",
        "Search by work order, pipe number, or status to review prior attempts and measurements.",
    )
    history_col1, history_col2, history_col3 = st.columns(3)
    with history_col1:
        history_production = st.text_input("Filter Production Number")
    with history_col2:
        history_pipe = st.text_input("Filter Pipe Number")
    with history_col3:
        history_status = st.selectbox(
            "Filter Status",
            ["", "in_progress", "completed", "rework", "scrapped"],
            format_func=lambda value: value or "All",
        )

    pipe_rows = search_pipe_units(
        branch=inspector["branch"],
        production_number=history_production or None,
        pipe_number=history_pipe or None,
        status=history_status or None,
    )
    if not pipe_rows:
        st.info("No pipe history records matched the current filters.")
        return

    st.dataframe(pipe_rows, use_container_width=True, hide_index=True)
    selected_pipe_id = st.selectbox(
        "Select Pipe Record",
        [row["id"] for row in pipe_rows],
        format_func=lambda pipe_id: next(
            f"{row['production_number']} | {row['pipe_number']} | {row['current_status']}"
            for row in pipe_rows
            if row["id"] == pipe_id
        ),
    )
    selected_pipe = next(row for row in pipe_rows if row["id"] == selected_pipe_id)
    st.caption(
        f"Selected pipe: {selected_pipe['production_number']} / {selected_pipe['pipe_number']} / "
        f"{selected_pipe['operation_description']}"
    )

    attempts = get_pipe_attempt_history(selected_pipe_id)
    if not attempts:
        st.info("No attempts were found for this pipe.")
        return

    st.markdown("**Attempt History**")
    st.dataframe(attempts, use_container_width=True, hide_index=True)

    selected_attempt_id = st.selectbox(
        "Select Attempt",
        [attempt["id"] for attempt in attempts],
        format_func=lambda attempt_id: next(
            f"Attempt #{attempt['attempt_no']} | {attempt['status']}"
            for attempt in attempts
            if attempt["id"] == attempt_id
        ),
    )
    measurements = get_attempt_measurements(selected_attempt_id)
    if measurements:
        st.markdown("**Measurement History**")
        st.dataframe(measurements, use_container_width=True, hide_index=True)


def render_ncr_tab(inspector):
    render_section_intro(
        "NCR Queue",
        "Review open NCRs, update disposition details, and close records when the pipe is resolved.",
    )
    ncr_status_filter = st.selectbox(
        "NCR Status",
        ["open", "closed", ""],
        format_func=lambda value: value or "All",
    )
    ncr_rows = get_ncr_reports(branch=inspector["branch"], status=ncr_status_filter or None)
    if not ncr_rows:
        st.info("No NCR records matched the current filter.")
        return

    st.dataframe(ncr_rows, use_container_width=True, hide_index=True)
    selected_ncr_id = st.selectbox(
        "Select NCR",
        [row["id"] for row in ncr_rows],
        format_func=lambda ncr_id: next(
            f"NCR #{row['id']} | Pipe {row['pipe_number']} | {row['status']}"
            for row in ncr_rows
            if row["id"] == ncr_id
        ),
    )
    selected_ncr = next(row for row in ncr_rows if row["id"] == selected_ncr_id)

    with st.form("ncr_update_form"):
        ncr_status = st.selectbox(
            "Status",
            ["open", "closed"],
            index=0 if selected_ncr["status"] == "open" else 1,
        )
        ncr_disposition = st.text_input("Disposition", value=selected_ncr.get("disposition") or "")
        ncr_tier = st.text_input("Tier Code", value=selected_ncr.get("tier_code") or "")
        ncr_nonconformance = st.text_area(
            "Nonconformance",
            value=selected_ncr.get("nonconformance") or "",
        )
        ncr_containment = st.text_area(
            "Immediate Containment",
            value=selected_ncr.get("immediate_containment") or "",
        )
        update_ncr = st.form_submit_button("Update NCR")

    if update_ncr:
        update_ncr_report(
            selected_ncr_id,
            status=ncr_status,
            disposition=ncr_disposition or None,
            tier_code=ncr_tier or None,
            nonconformance=ncr_nonconformance or None,
            immediate_containment=ncr_containment or None,
        )
        st.success(f"NCR #{selected_ncr_id} updated.")
        st.rerun()


def render_workflow():
    inspector = st.session_state.inspector
    session_record = st.session_state.session_record

    render_hero_panel(
        "AutoIRR Workflow",
        "Run inspections, review pipe history, and manage NCR follow-up from one workspace.",
        badges=[
            f"Inspector: {inspector['name']}",
            f"Role: {get_user_role_label(inspector)}",
            f"Branch: {inspector['branch'] or 'Unknown'}",
            f"Department: {inspector.get('department') or 'Unknown'}",
            f"Shift: {st.session_state.shift}",
        ],
    )

    col1, col2 = st.columns([5, 1])
    with col2:
        if st.button("Log Out", use_container_width=True):
            close_inspector_session(session_record["id"])
            st.session_state.session_record = None
            st.session_state.inspector = None
            st.session_state.active_inspection = None
            st.session_state.pending_login_context = None
            st.rerun()

    inspection_tab, pipe_history_tab, ncr_tab = st.tabs(
        ["Inspection Entry", "Pipe History", "NCR Queue"]
    )

    with inspection_tab:
        render_inspection_tab(inspector, session_record)
    with pipe_history_tab:
        render_pipe_history_tab(inspector)
    with ncr_tab:
        render_ncr_tab(inspector)


def main():
    _init_state()
    inject_custom_styles()
    if st.session_state.session_record and st.session_state.inspector:
        available_modes = ["Inspector Workflow"]
        if is_admin_user(st.session_state.inspector):
            available_modes.append("Admin Tools")

        with st.sidebar:
            st.markdown(
                f"**User:** {st.session_state.inspector['name']}  \n"
                f"**Role:** {get_user_role_label(st.session_state.inspector)}"
            )
            app_mode = st.radio("Mode", available_modes)

        if app_mode == "Admin Tools":
            render_admin()
        else:
            render_workflow()
    else:
        render_login()


if __name__ == "__main__":
    main()
