import json


async def prepare_merge_data(
    main_contact: dict,
    duplicate_contacts: list,
    priority_fields: list,
) -> dict:
    """
    Подготавливает данные для merge-запроса в amoCRM с учетом:
      - Формирования списка ID для слияния.
      - Объединения тегов из всех контактов.
      - Обработки кастомных полей:
          • Для телефонов и email создаются JSON-строки с DESCRIPTION и VALUE.
          • Остальные поля берутся как есть (с приоритетом из самого нового дубликата при наличии).
      - Добавления компании, если она есть.
      - Получения сделок для контактов.
    """
    all_contacts = [main_contact] + duplicate_contacts
    ids_to_merge = [contact["id"] for contact in all_contacts]

    final_data = {}
    final_data["id[]"] = ids_to_merge
    final_data["result_element[NAME]"] = main_contact.get("name", "")
    if main_contact.get("responsible_user_id"):
        final_data["result_element[MAIN_USER_ID]"] = main_contact["responsible_user_id"]

    # Объединяем теги (если есть)
    all_tags = set()
    for contact in all_contacts:
        tags = contact.get("_embedded", {}).get("tags", [])
        for tag in tags:
            all_tags.add(tag["id"])
    if all_tags:
        final_data["result_element[TAGS][]"] = list(all_tags)

    # Обработка кастомных полей
    custom_fields = {}

    # Функция для обработки полей с multitext (PHONE, EMAIL) – возвращает список словарей
    def process_multitext_field(field):
        entries = []
        for value_obj in field.get("values", []):
            entry = {
                "DESCRIPTION": value_obj.get("enum_code", "WORK"),
                "VALUE": value_obj.get("value"),
            }
            entries.append(entry)
        return entries

    # Сначала берем поля из основного контакта
    for field in main_contact.get("custom_fields_values", []):
        field_id = field.get("field_id")
        if not field_id or "values" not in field:
            continue
        if field.get("field_code") in ["PHONE", "EMAIL"]:
            custom_fields[field_id] = process_multitext_field(field)
        else:
            custom_fields[field_id] = field["values"][0]["value"]

    # Обновляем приоритетными полями из самого нового дубликата
    if duplicate_contacts:
        newest_contact = duplicate_contacts[-1]
        for field in newest_contact.get("custom_fields_values", []):
            field_id = field.get("field_id")
            field_name = field.get("field_name")
            for pf in priority_fields:
                if pf.get("field_name") == field_name and pf.get("action"):
                    if field.get("field_code") in ["PHONE", "EMAIL"]:
                        custom_fields[field_id] = process_multitext_field(field)
                    else:
                        custom_fields[field_id] = field["values"][0]["value"]

    # Добавляем кастомные поля в итоговый запрос
    for field_id, field_value in custom_fields.items():
        # Если значение список, то формируем ключ с [] и сериализуем каждую запись в JSON
        if isinstance(field_value, list):
            final_data[f"result_element[cfv][{field_id}][]"] = [
                json.dumps(item, ensure_ascii=False) for item in field_value
            ]
        else:
            final_data[f"result_element[cfv][{field_id}]"] = field_value

    # Добавляем ID основного контакта
    final_data["result_element[ID]"] = main_contact["id"]

    # Обработка компании: если у основного контакта есть компании, берем первую
    companies = main_contact.get("_embedded", {}).get("companies", [])
    if companies:
        company = companies[0]
        company_id = company.get("id")
        if company_id:
            final_data["double[companies][result_element][COMPANY_UID]"] = company_id
            final_data["double[companies][result_element][ID]"] = company_id

    lead_ids_set = set()
    for contact in all_contacts:
        leads = contact.get("_embedded", {}).get("leads", [])
        for lead in leads:
            if lead.get("id"):
                lead_ids_set.add(lead["id"])
    if lead_ids_set:
        final_data["result_element[LEADS][]"] = list(lead_ids_set)
    return final_data
