import { Type } from 'avsc';

export const messageSchema = Type.forSchema({
    type: 'record',
    name: 'Message',
    fields: [
        { name: "id", type: "int" },
        { name: "email", type: "string" },
        { name: "message", type: "string" },
        { name: "createdAt", type: "string" },
    ],
});
