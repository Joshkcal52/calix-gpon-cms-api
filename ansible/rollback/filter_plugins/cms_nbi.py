"""
Filters for rollback.yml: read CMS NBI replies and work out what to restore.

Kept free of any dependency on the API code in this repository, so the
rollback playbook runs from a bare checkout with nothing but Ansible.
"""

from xml.etree import ElementTree

from ansible.errors import AnsibleFilterError

VENDOR = "CXNK"


def _root(content):
    try:
        root = ElementTree.fromstring(content)
    except ElementTree.ParseError as e:
        raise AnsibleFilterError(f"CMS sent a reply that is not valid XML: {e}")
    for element in root.iter():
        if "}" in element.tag:
            element.tag = element.tag.split("}", 1)[1]
    return root


def _text(element, path):
    found = element.find(path)
    return (found.text or "").strip() if found is not None else ""


def _vlan(value):
    return int(value) if value.isdigit() else None


def cms_serno(serial_number):
    """CXNK00A3F104 -> A3F104, the form CMS stores"""
    serial_number = str(serial_number)
    body = serial_number[4:] if serial_number[:4].isalpha() else serial_number
    return body.lstrip("0").upper() or "0"


def cms_session_id(content):
    """The session ID from a login reply. Fails if CMS refused the login."""
    root = _root(content)
    code, session = _text(root, ".//auth-reply/ResultCode"), _text(root, ".//auth-reply/SessionId")
    if code != "0" or not session:
        raise AnsibleFilterError(
            f"CMS refused the login (result code {code or 'missing'}). Check CMS_NBI_USER and CMS_NBI_PASSWORD.")
    return session


def cms_rpc_error(content):
    """The error message in a CMS reply, or an empty string if there is none"""
    if "rpc-error" not in content:
        return ""
    root = _root(content)
    return _text(root, ".//rpc-error/error-message") or _text(root, ".//rpc-error/error-tag") or "CMS returned an error"


def cms_onts(content):
    """The ONTs in a show-ont reply"""
    onts = []
    for obj in _root(content).iter("object"):
        if _text(obj, "./type") != "Ont":
            continue
        pon = [_text(obj, f"./linked-pon/id/{part}") for part in ("shelf", "card", "gponport")]
        serno = _text(obj, "./serno")
        onts.append({
            "ont_id": int(_text(obj, "./id/ont")),
            "serial_number": f"{_text(obj, './vendor') or VENDOR}{serno.upper().zfill(8)}",
            "profile": _text(obj, "./ontprof/id/ontprof"),
            "admin_state": _text(obj, "./admin"),
            "subscriber_id": _text(obj, "./subscr-id") or None,
            "description": _text(obj, "./descr") or None,
            "pon_port": "/".join(pon) if all(pon) else None,
        })
    return onts


def cms_services(content):
    """The Ethernet services in a reply to templates/services_get.xml.j2"""
    services = []
    for child in _root(content).iter("child"):
        if _text(child, "./type") != "EthSvc":
            continue
        services.append({
            "ont_port": int(_text(child, "./id/ontethany")),
            "service_index": int(_text(child, "./id/ethsvc")),
            "vlan": _vlan(_text(child, "./out-tag")),
            "inner_vlan": _vlan(_text(child, "./in-tag")),
            "bandwidth_profile": _text(child, "./bw-prof/id/bwprof") or None,
            "tag_action": _text(child, "./tag-action/id/svctagaction") or None,
            "admin_state": _text(child, "./admin") or "enabled",
            "description": _text(child, "./descr") or None,
        })
    return services


def rollback_scope(onts, card="", interface="", ont=""):
    """
    Narrow a snapshot's ONTs to what the engineer asked for.
      card       shelf/card, e.g. 1/2
      interface  shelf/card/port, e.g. 1/2/3
      ont        a single ONT ID
    """
    selected = []
    for item in onts:
        pon = str(item.get("pon_port") or "")
        if card and not pon.startswith(f"{card}/"):
            continue
        if interface and pon != str(interface):
            continue
        if ont and str(item.get("ont_id")) != str(ont):
            continue
        selected.append(item)
    return selected


ONT_FIELDS = ("serial_number", "profile", "admin_state", "subscriber_id", "description")
SERVICE_FIELDS = ("vlan", "inner_vlan", "bandwidth_profile", "tag_action", "admin_state", "description")


def _differences(wanted, live, fields):
    return [f"{field}: {live.get(field)!r} -> {wanted.get(field)!r}"
            for field in fields if (wanted.get(field) or None) != (live.get(field) or None)]


def rollback_actions(snapshot_ont, live_ont, live_services, remove_extra_services=True):
    """
    The ordered list of changes that make one live ONT match its snapshot.
    Each action names the request template to send and says, in words, what it does.
    An empty list means the ONT already matches.
    """
    ont_id = snapshot_ont["ont_id"]
    actions = []

    if not live_ont:
        actions.append({"template": "ont_create.xml.j2", "service": {},
                        "summary": f"create ONT {ont_id} ({snapshot_ont.get('serial_number')})"})
        live_services = []
    else:
        changed = _differences(snapshot_ont, live_ont, ONT_FIELDS)
        if changed:
            actions.append({"template": "ont_merge.xml.j2", "service": {},
                            "summary": f"update ONT {ont_id} ({'; '.join(changed)})"})

    def key(service):
        return (int(service["ont_port"]), int(service["service_index"]))

    wanted = {key(s): s for s in snapshot_ont.get("services") or []}
    live = {key(s): s for s in live_services or []}

    if remove_extra_services:
        for position in sorted(set(live) - set(wanted)):
            actions.append({"template": "service_delete.xml.j2", "service": live[position],
                            "summary": f"remove service {position[1]} on port {position[0]} of ONT {ont_id} "
                                       f"(not in the snapshot)"})
    for position in sorted(wanted):
        service = wanted[position]
        if position not in live:
            actions.append({"template": "service_create.xml.j2", "service": service,
                            "summary": f"create service {position[1]} on port {position[0]} of ONT {ont_id} "
                                       f"(VLAN {service.get('vlan')})"})
        else:
            changed = _differences(service, live[position], SERVICE_FIELDS)
            if changed:
                actions.append({"template": "service_merge.xml.j2", "service": service,
                                "summary": f"update service {position[1]} on port {position[0]} of ONT {ont_id} "
                                           f"({'; '.join(changed)})"})
    return actions


def rollback_results(host_names, hostvars, dry_run=False):
    """The record kept for each ONT in the final report, built from what each host registered"""
    results = []
    for name in host_names:
        facts = hostvars[name]
        attempted = (facts.get("applied") or {}).get("results") or []
        changes = [r["item"]["summary"] for r in attempted if isinstance(r.get("item"), dict)]
        error = facts.get("rollback_error") or ""
        if error:
            status = "failed"
        elif not changes:
            status = "unchanged"
        else:
            status = "would_change" if dry_run else "restored"
        result = {"ont_id": int(facts["snapshot_ont"]["ont_id"]), "status": status, "changes": changes}
        if error:
            result["error"] = error
        results.append(result)
    return results


def rollback_failure(failed_result, task_name=""):
    """One readable sentence for why a task failed, from Ansible's ansible_failed_result"""
    results = failed_result.get("results") or [failed_result]
    for result in results:
        if not result.get("failed"):
            continue
        reason = ""
        content = result.get("content") or ""
        if content:
            try:
                reason = cms_rpc_error(content)
            except AnsibleFilterError:
                reason = ""
        reason = reason or result.get("msg") or "no details"
        doing = (result.get("item") or {}).get("summary") if isinstance(result.get("item"), dict) else ""
        return f"{doing or task_name or 'request'} failed: {reason}"
    return f"{task_name or 'request'} failed: {failed_result.get('msg', 'no details')}"


class FilterModule:
    def filters(self):
        return {
            "cms_serno": cms_serno,
            "cms_session_id": cms_session_id,
            "cms_rpc_error": cms_rpc_error,
            "cms_onts": cms_onts,
            "cms_services": cms_services,
            "rollback_scope": rollback_scope,
            "rollback_actions": rollback_actions,
            "rollback_failure": rollback_failure,
            "rollback_results": rollback_results,
        }
