import xml.etree.ElementTree as ET

def count_records(xml_file_path):
    count = 0
    context = ET.iterparse(xml_file_path, events=('start', 'end'))
    context = iter(context)
    _, root = next(context)

    for event, elem in context:
        count += 1
        if event == 'end' and elem.tag == 'record':
            elem.clear()

    return count

xml_file_path = 'D:/AML Services/World Checker XML File/premium-world-check-full.xml'
records_count = count_records(xml_file_path)
print(f'Total records: {records_count}')
# 5,347,099
# Total records: 577,576,977
