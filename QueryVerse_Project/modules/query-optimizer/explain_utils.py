def parse_explain_output(cursor_result):
    return "\n".join([row[0] for row in cursor_result])
