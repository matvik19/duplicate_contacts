import json


async def prepare_merge_data(
    main_contact: dict, duplicate_contacts: list, priority_fields: list
) -> dict:
    """
    Подготавливает данные для merge-запроса в amoCRM с учетом:
      - Формирования списка ID для слияния.
      - Объединения тегов из всех контактов.
      - Обработки кастомных полей:
          • Для телефонов и email создаются JSON-строки с DESCRIPTION и VALUE.
          • Остальные поля берутся как есть (с приоритетом из самого нового дубликата, если указан).
      - Добавления компании, если она есть.
      - Добавления сделок для контактов (только их ID).
    """
    # Собираем все контакты (основной + дубликаты)
    all_contacts = [main_contact] + duplicate_contacts
    final_data = {}

    # Список ID контактов для слияния
    final_data["id[]"] = [contact["id"] for contact in all_contacts]

    # Основные поля контакта
    final_data["result_element[NAME]"] = main_contact.get("name", "")
    if main_contact.get("responsible_user_id"):
        final_data["result_element[MAIN_USER_ID]"] = main_contact["responsible_user_id"]
    final_data["result_element[ID]"] = main_contact["id"]

    # Объединяем теги из всех контактов
    all_tags = set()
    for contact in all_contacts:
        tags = contact.get("_embedded", {}).get("tags", [])
        for tag in tags:
            tag_id = tag.get("id")
            if tag_id:
                all_tags.add(tag_id)
    if all_tags:
        final_data["result_element[TAGS][]"] = list(all_tags)

    # Извлекаем кастомные поля из основного контакта
    custom_fields = extract_custom_fields(main_contact)

    # Обновляем кастомные поля приоритетными данными из самого нового дубликата
    if duplicate_contacts:
        newest_contact = duplicate_contacts[-1]
        new_fields = extract_custom_fields(newest_contact)
        for field_id, value in new_fields.items():
            field_name = get_field_name_by_id(main_contact, field_id) or ""
            for pf in priority_fields:
                if pf.get("field_name") == field_name and pf.get("action"):
                    custom_fields[field_id] = value
                    break

    # Добавляем кастомные поля в итоговый запрос
    for field_id, value in custom_fields.items():
        if isinstance(value, list):
            final_data[f"result_element[cfv][{field_id}][]"] = [
                json.dumps(item, ensure_ascii=False) for item in value
            ]
        else:
            final_data[f"result_element[cfv][{field_id}]"] = value

    # Обработка компании: если у основного контакта есть компании, берём первую
    companies = main_contact.get("_embedded", {}).get("companies", [])
    if companies:
        company = companies[0]
        company_id = company.get("id")
        if company_id:
            final_data["double[companies][result_element][COMPANY_UID]"] = company_id
            final_data["double[companies][result_element][ID]"] = company_id

    # Собираем сделки (LEADS) из всех контактов, оставляя только их ID
    lead_ids = set()
    for contact in all_contacts:
        leads = contact.get("_embedded", {}).get("leads", [])
        for lead in leads:
            lead_id = lead.get("id")
            if lead_id:
                lead_ids.add(lead_id)
    if lead_ids:
        final_data["result_element[LEADS][]"] = list(lead_ids)

    return final_data


def process_multi_text_field(field: dict) -> list:
    """
    Обрабатывает поля с multitext (например, PHONE, EMAIL) и возвращает список словарей
    с ключами DESCRIPTION и VALUE, повторяя каждый элемент 'repeat_count' раз.
    """
    entries = []
    values = field.get("values", [])

    # Повторяем значения столько раз, сколько их в поле
    for value_obj in values:
        entry = {
            "DESCRIPTION": value_obj.get("enum_code", "WORK"),
            "VALUE": value_obj.get("value"),
        }
        entries.append(entry)

    return entries


def extract_custom_fields(contact: dict) -> dict:
    """
    Извлекает кастомные поля контакта и возвращает словарь вида {field_id: value}.
    Для PHONE и EMAIL используется process_multitext_field, для остальных берется первое значение.
    """
    fields = {}
    for field in contact.get("custom_fields_values", []):
        field_id = field.get("field_id")
        if not field_id or "values" not in field:
            continue
        if field.get("field_code") in ["PHONE", "EMAIL"]:
            fields[field_id] = process_multi_text_field(field)
        else:
            fields[field_id] = field["values"][0].get("value")
    return fields


def get_field_name_by_id(contact: dict, field_id) -> str | None:
    """
    Ищет в custom_fields_values контакта поле с заданным field_id и возвращает его имя.
    """
    for field in contact.get("custom_fields_values", []):
        if field.get("field_id") == field_id:
            return field.get("field_name")
    return None
