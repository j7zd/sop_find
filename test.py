import api

if __name__ == "__main__":
    servers = ['localhost:29092']
    usr_id = 0
    teacher_id = 1
    s = api.SOP_FIND(servers, usr_id)
    response = s.get_students_for_teacher(teacher_id)
    print("response:", response)
    response = s.get_students_for_teacher(teacher_id)
    print("response:", response)