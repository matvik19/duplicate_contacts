import json


async def prepare_merge_data(
    main_contact: dict, duplicates: list[dict], priority_fields: list[dict]
) -> dict[str, any]:
    """Подготавливает данные для слияния контактов в amoCRM."""
    all_contacts = [main_contact] + duplicates
    payload = {
        "id[]": [c["id"] for c in all_contacts],
        "result_element[ID]": main_contact["id"],
    }

    # Обработка стандартных полей с учетом приоритета
    standard_fields_map = {
        "name": "NAME",
        "responsible_user_id": "MAIN_USER_ID",
        "created_at": "DATE_CREATE",
        "price": "PRICE",
    }

    priority_field_names = {pf["field_name"] for pf in priority_fields}
    youngest = duplicates[-1] if duplicates else None

    for field, amo_key in standard_fields_map.items():
        value = None
        if (
            field in priority_field_names
            and youngest
            and youngest.get(field) is not None
        ):
            value = youngest.get(field)
        elif main_contact.get(field) is not None:
            value = main_contact.get(field)

        if value is not None:
            payload[f"result_element[{amo_key}]"] = value

    payload.update(_merge_tags(all_contacts))
    custom_fields = _merge_custom_fields(main_contact, duplicates, priority_fields)
    payload.update(_format_custom_fields(custom_fields))
    payload.update(_merge_companies(main_contact))
    payload.update(_merge_leads(all_contacts))
    return payload


def _merge_tags(contacts: list[dict]) -> dict[str, list[int]]:
    """Объединяет теги из всех контактов."""
    tags = {
        tag["id"]
        for c in contacts
        for tag in c.get("_embedded", {}).get("tags", [])
        if tag.get("id")
    }
    return {"result_element[TAGS][]": list(tags)} if tags else {}


def _merge_custom_fields(
    main_contact: dict, duplicates: list[dict], priority_fields: list[dict]
) -> dict[int, any]:
    """Объединяет кастомные поля с учетом приоритетов и уникальности телефонов."""
    fields = extract_custom_fields(main_contact)

    # Преобразуем в множество имён полей, которые нужно заменять
    priority_field_names = {pf["field_name"] for pf in priority_fields}

    # Берем из младшего дубля нужные приоритетные поля
    if duplicates:
        newest = duplicates[-1]
        for field_id, value in extract_custom_fields(newest).items():
            field_name = get_field_name_by_id(main_contact, field_id) or ""
            if field_name in priority_field_names:
                fields[field_id] = value

    for dup in duplicates:
        for field_id, value in extract_custom_fields(dup).items():
            if field_id not in fields:
                fields[field_id] = value
            elif get_field_code_by_id(dup, field_id) == "PHONE":
                fields[field_id] = _merge_phones(fields[field_id], value)

    return fields


def _merge_phones(existing: list[dict], new: list[dict]) -> list[dict]:
    """Объединяет номера телефонов, оставляя только уникальные."""
    normalized = {normalize_phone(p["VALUE"]) for p in existing if p.get("VALUE")}
    return existing + [p for p in new if normalize_phone(p["VALUE"]) not in normalized]


def _format_custom_fields(fields: dict[int, any]) -> dict[str, any]:
    """Форматирует кастомные поля для payload."""
    return {
        f"result_element[cfv][{field_id}][]": (
            [json.dumps(item, ensure_ascii=False) for item in value]
            if isinstance(value, list)
            else value
        )
        for field_id, value in fields.items()
    }


def _merge_companies(main_contact: dict) -> dict[str, int]:
    """Добавляет первую компанию из главного контакта."""
    companies = main_contact.get("_embedded", {}).get("companies", [])
    if companies and (company_id := companies[0].get("id")):
        return {
            "double[companies][result_element][COMPANY_UID]": company_id,
            "double[companies][result_element][ID]": company_id,
        }
    return {}


def _merge_leads(contacts: list[dict]) -> dict[str, list[int]]:
    """Объединяет ID сделок из всех контактов."""
    leads = {
        lead["id"]
        for c in contacts
        for lead in c.get("_embedded", {}).get("leads", [])
        if lead.get("id")
    }
    return {"result_element[LEADS][]": list(leads)} if leads else {}


def extract_custom_fields(contact: dict) -> dict[int, any]:
    """Извлекает кастомные поля контакта."""
    return {
        f["field_id"]: (
            process_multi_text_field(f)
            if f.get("field_code") in ["PHONE", "EMAIL"]
            else f["values"][0]["value"]
        )
        for f in contact.get("custom_fields_values", [])
        if f.get("field_id") and f.get("values")
    }


def process_multi_text_field(field: dict) -> list[dict]:
    """Обрабатывает мультитекст-поля."""
    return [
        {"DESCRIPTION": v.get("enum_code", "WORK"), "VALUE": v["value"]}
        for v in field.get("values", [])
    ]


def get_field_name_by_id(contact: dict, field_id: int) -> str | None:
    return next(
        (
            f["field_name"]
            for f in contact.get("custom_fields_values", [])
            if f.get("field_id") == field_id
        ),
        None,
    )


def get_field_code_by_id(contact: dict, field_id: int) -> str | None:
    return next(
        (
            f["field_code"]
            for f in contact.get("custom_fields_values", [])
            if f.get("field_id") == field_id
        ),
        None,
    )


def normalize_phone(phone: str) -> str:
    """
    Удаляет из номера телефона все символы, кроме цифр.
    Если номер состоит из 11 цифр и начинается с '8', заменяет её на '7'.
    """
    digits = "".join(c for c in phone if c.isdigit())
    if len(digits) == 11 and digits.startswith("8"):
        digits = "7" + digits[1:]
    return digits
